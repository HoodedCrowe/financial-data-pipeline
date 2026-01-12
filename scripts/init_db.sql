-- Financial Data Pipeline - Database Initialization
-- This script runs automatically when the container first starts

-- Enable TimescaleDB extension
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- =============================================================================
-- CORE MARKET DATA TABLE
-- =============================================================================
-- Design decisions:
-- 1. Composite primary key (timestamp, symbol, source) enables UPSERT idempotency
-- 2. Source tracking allows cross-source validation
-- 3. Metadata JSONB for extensibility without schema changes

CREATE TABLE IF NOT EXISTS market_data (
    timestamp       TIMESTAMPTZ NOT NULL,
    symbol          VARCHAR(20) NOT NULL,
    source          VARCHAR(50) NOT NULL,
    open            DECIMAL(18, 6) NOT NULL,
    high            DECIMAL(18, 6) NOT NULL,
    low             DECIMAL(18, 6) NOT NULL,
    close           DECIMAL(18, 6) NOT NULL,
    volume          BIGINT NOT NULL,
    adjusted_close  DECIMAL(18, 6),
    metadata        JSONB DEFAULT '{}',
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    
    -- Composite PK enables idempotent upserts
    PRIMARY KEY (timestamp, symbol, source)
);

-- Convert to TimescaleDB hypertable for automatic time partitioning
-- chunk_time_interval: 7 days balances query performance vs chunk overhead
SELECT create_hypertable(
    'market_data',
    'timestamp',
    chunk_time_interval => INTERVAL '7 days',
    if_not_exists => TRUE
);

-- =============================================================================
-- DATA QUALITY METRICS TABLE
-- =============================================================================
-- Stores results from Great Expectations validation runs
-- Critical for tracking data quality over time

CREATE TABLE IF NOT EXISTS quality_metrics (
    id                  SERIAL,
    run_timestamp       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    source              VARCHAR(50) NOT NULL,
    suite_name          VARCHAR(100) NOT NULL,
    expectation_type    VARCHAR(200) NOT NULL,
    column_name         VARCHAR(100),
    success             BOOLEAN NOT NULL,
    observed_value      TEXT,
    expected_value      TEXT,
    details             JSONB DEFAULT '{}',
    created_at          TIMESTAMPTZ DEFAULT NOW(),

    -- Composite primary key includes partitioning column for TimescaleDB
    PRIMARY KEY (id, run_timestamp)
);

-- Convert to hypertable for efficient time-based queries on quality data
SELECT create_hypertable(
    'quality_metrics',
    'run_timestamp',
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists => TRUE
);

-- =============================================================================
-- INGESTION LOGS TABLE
-- =============================================================================
-- Track every ingestion run for debugging and monitoring

CREATE TABLE IF NOT EXISTS ingestion_logs (
    id              SERIAL,
    run_timestamp   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    source          VARCHAR(50) NOT NULL,
    symbol          VARCHAR(20) NOT NULL,
    status          VARCHAR(20) NOT NULL, -- 'success', 'partial', 'failed'
    rows_fetched    INTEGER DEFAULT 0,
    rows_inserted   INTEGER DEFAULT 0,
    rows_updated    INTEGER DEFAULT 0,
    duration_ms     INTEGER,
    error_message   TEXT,
    metadata        JSONB DEFAULT '{}',

    -- Composite primary key includes partitioning column for TimescaleDB
    PRIMARY KEY (id, run_timestamp)
);

SELECT create_hypertable(
    'ingestion_logs',
    'run_timestamp',
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists => TRUE
);

-- =============================================================================
-- INDEXES
-- =============================================================================
-- Strategic indexes for common query patterns

-- Fast lookups by symbol (most common query pattern)
CREATE INDEX IF NOT EXISTS idx_market_data_symbol_time 
    ON market_data (symbol, timestamp DESC);

-- Fast lookups by source for cross-source comparison
CREATE INDEX IF NOT EXISTS idx_market_data_source_time 
    ON market_data (source, timestamp DESC);

-- Quality metrics dashboard queries
CREATE INDEX IF NOT EXISTS idx_quality_metrics_source_time 
    ON quality_metrics (source, run_timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_quality_metrics_success 
    ON quality_metrics (success, run_timestamp DESC);

-- =============================================================================
-- VIEWS FOR COMMON QUERIES
-- =============================================================================

-- Latest price per symbol (useful for dashboards)
CREATE OR REPLACE VIEW v_latest_prices AS
SELECT DISTINCT ON (symbol, source)
    symbol,
    source,
    timestamp,
    open,
    high,
    low,
    close,
    volume
FROM market_data
ORDER BY symbol, source, timestamp DESC;

-- Daily quality summary
CREATE OR REPLACE VIEW v_quality_summary AS
SELECT 
    date_trunc('day', run_timestamp) AS date,
    source,
    COUNT(*) AS total_checks,
    SUM(CASE WHEN success THEN 1 ELSE 0 END) AS passed,
    SUM(CASE WHEN NOT success THEN 1 ELSE 0 END) AS failed,
    ROUND(100.0 * SUM(CASE WHEN success THEN 1 ELSE 0 END) / COUNT(*), 2) AS pass_rate
FROM quality_metrics
GROUP BY date_trunc('day', run_timestamp), source
ORDER BY date DESC, source;

-- Cross-source price comparison (for detecting discrepancies)
CREATE OR REPLACE VIEW v_cross_source_comparison AS
WITH source_prices AS (
    SELECT 
        timestamp,
        symbol,
        source,
        close,
        ROW_NUMBER() OVER (PARTITION BY timestamp, symbol ORDER BY source) as rn
    FROM market_data
    WHERE timestamp > NOW() - INTERVAL '7 days'
)
SELECT 
    a.timestamp,
    a.symbol,
    a.source AS source_a,
    b.source AS source_b,
    a.close AS price_a,
    b.close AS price_b,
    ABS(a.close - b.close) AS price_diff,
    CASE 
        WHEN a.close > 0 THEN ROUND(100.0 * ABS(a.close - b.close) / a.close, 4)
        ELSE 0
    END AS pct_diff
FROM source_prices a
JOIN source_prices b ON a.timestamp = b.timestamp 
    AND a.symbol = b.symbol 
    AND a.source < b.source;

-- =============================================================================
-- HELPER FUNCTIONS
-- =============================================================================

-- Function for batch upsert (used by ingestion scripts)
CREATE OR REPLACE FUNCTION upsert_market_data(
    p_timestamp TIMESTAMPTZ,
    p_symbol VARCHAR,
    p_source VARCHAR,
    p_open DECIMAL,
    p_high DECIMAL,
    p_low DECIMAL,
    p_close DECIMAL,
    p_volume BIGINT,
    p_adjusted_close DECIMAL DEFAULT NULL,
    p_metadata JSONB DEFAULT '{}'
) RETURNS VOID AS $$
BEGIN
    INSERT INTO market_data (timestamp, symbol, source, open, high, low, close, volume, adjusted_close, metadata)
    VALUES (p_timestamp, p_symbol, p_source, p_open, p_high, p_low, p_close, p_volume, p_adjusted_close, p_metadata)
    ON CONFLICT (timestamp, symbol, source) 
    DO UPDATE SET
        open = EXCLUDED.open,
        high = EXCLUDED.high,
        low = EXCLUDED.low,
        close = EXCLUDED.close,
        volume = EXCLUDED.volume,
        adjusted_close = EXCLUDED.adjusted_close,
        metadata = EXCLUDED.metadata;
END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- TIMESCALEDB OPTIMIZATIONS
-- =============================================================================

-- Enable compression on market_data (significant storage savings)
-- Compression works on chunks older than 7 days
ALTER TABLE market_data SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'symbol, source'
);

-- Create compression policy: compress chunks older than 7 days
SELECT add_compression_policy('market_data', INTERVAL '7 days', if_not_exists => TRUE);

-- Create retention policy: keep 2 years of data (adjust as needed)
-- Uncomment if you want automatic data retention
-- SELECT add_retention_policy('market_data', INTERVAL '2 years', if_not_exists => TRUE);

-- =============================================================================
-- SAMPLE DATA (for testing - remove in production)
-- =============================================================================

-- Insert a few rows so you can verify the setup works
INSERT INTO market_data (timestamp, symbol, source, open, high, low, close, volume)
VALUES 
    (NOW() - INTERVAL '1 day', 'AAPL', 'test_source', 150.00, 152.50, 149.00, 151.75, 1000000),
    (NOW() - INTERVAL '1 day', 'GOOGL', 'test_source', 140.00, 142.00, 139.50, 141.25, 500000),
    (NOW(), 'AAPL', 'test_source', 151.75, 153.00, 151.00, 152.50, 800000)
ON CONFLICT DO NOTHING;

-- Verify setup
DO $$
BEGIN
    RAISE NOTICE '========================================';
    RAISE NOTICE 'Financial Data Pipeline - DB Initialized';
    RAISE NOTICE '========================================';
    RAISE NOTICE 'Tables created: market_data, quality_metrics, ingestion_logs';
    RAISE NOTICE 'Hypertables enabled with 7-day chunking';
    RAISE NOTICE 'Compression policy: chunks > 7 days';
    RAISE NOTICE 'Sample data inserted for testing';
    RAISE NOTICE '========================================';
END $$;
