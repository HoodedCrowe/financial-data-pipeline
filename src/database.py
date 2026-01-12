"""
Database Operations Module

This module handles all database interactions for the financial data pipeline.
Key responsibilities:
1. Connection management with pooling
2. Batch insertions for performance
3. Idempotent upserts (safe reruns)
4. Query helpers for common operations

Design decisions:
- Uses psycopg2 for synchronous operations (simpler for batch jobs)
- Batch size of 1000 rows balances memory usage vs round-trip overhead
- UPSERT pattern with ON CONFLICT ensures idempotency
"""

import os
from contextlib import contextmanager
from datetime import datetime
from decimal import Decimal
from typing import Generator, Optional

import psycopg2
from psycopg2.extras import execute_values, RealDictCursor
import structlog

from src.models import OHLCVRecord, DataSource, QualityCheckResult


logger = structlog.get_logger(__name__)

# Default batch size for inserts
DEFAULT_BATCH_SIZE = 1000


def get_connection_params() -> dict:
    """
    Get database connection parameters from environment.
    
    Returns:
        Dictionary of connection parameters for psycopg2.
    """
    return {
        "host": os.getenv("POSTGRES_HOST", "localhost"),
        "port": int(os.getenv("POSTGRES_PORT", "5432")),
        "database": os.getenv("POSTGRES_DB", "financial_data"),
        "user": os.getenv("POSTGRES_USER", "findata"),
        "password": os.getenv("POSTGRES_PASSWORD", "findata_secret"),
    }


@contextmanager
def get_db_connection() -> Generator[psycopg2.extensions.connection, None, None]:
    """
    Context manager for database connections.
    
    Ensures connections are properly closed, even on exceptions.
    
    Usage:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM market_data")
    """
    conn = None
    try:
        conn = psycopg2.connect(**get_connection_params())
        yield conn
    finally:
        if conn is not None:
            conn.close()


@contextmanager
def get_db_cursor(dict_cursor: bool = False):
    """
    Context manager for database cursor with automatic commit/rollback.
    
    Args:
        dict_cursor: If True, return results as dictionaries instead of tuples.
    
    Usage:
        with get_db_cursor(dict_cursor=True) as cur:
            cur.execute("SELECT * FROM market_data")
            rows = cur.fetchall()  # List of dicts
    """
    with get_db_connection() as conn:
        cursor_factory = RealDictCursor if dict_cursor else None
        with conn.cursor(cursor_factory=cursor_factory) as cur:
            try:
                yield cur
                conn.commit()
            except Exception:
                conn.rollback()
                raise


class MarketDataRepository:
    """
    Repository for market data operations.
    
    Implements the Repository pattern to encapsulate database logic.
    This keeps data access code organized and testable.
    """
    
    def __init__(self, batch_size: int = DEFAULT_BATCH_SIZE):
        """
        Initialize repository.
        
        Args:
            batch_size: Number of rows per batch insert.
        """
        self.batch_size = batch_size
        self._logger = structlog.get_logger(self.__class__.__name__)
    
    def insert_records(self, records: list[OHLCVRecord]) -> tuple[int, int]:
        """
        Insert OHLCV records with upsert (idempotent).
        
        Uses ON CONFLICT DO NOTHING to skip duplicates. This is safer than
        DO UPDATE for historical data that shouldn't change.
        
        Args:
            records: List of OHLCVRecord objects to insert.
            
        Returns:
            Tuple of (rows_attempted, rows_inserted).
            
        Why ON CONFLICT DO NOTHING vs DO UPDATE?
        - Historical data rarely changes legitimately
        - DO UPDATE could mask data quality issues
        - Easier to debug when counts differ (indicates duplicates)
        """
        if not records:
            return 0, 0
        
        # Prepare data for batch insert
        values = [
            (
                r.timestamp,
                r.symbol,
                r.source.value,
                float(r.open),
                float(r.high),
                float(r.low),
                float(r.close),
                r.volume,
                float(r.adjusted_close) if r.adjusted_close else None,
                psycopg2.extras.Json(r.metadata),
            )
            for r in records
        ]
        
        insert_sql = """
            INSERT INTO market_data 
            (timestamp, symbol, source, open, high, low, close, volume, adjusted_close, metadata)
            VALUES %s
            ON CONFLICT (timestamp, symbol, source) DO NOTHING
        """
        
        total_inserted = 0
        
        # Process in batches
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                for i in range(0, len(values), self.batch_size):
                    batch = values[i:i + self.batch_size]
                    execute_values(cur, insert_sql, batch)
                    total_inserted += cur.rowcount
                conn.commit()
        
        self._logger.info(
            "batch_insert_complete",
            rows_attempted=len(records),
            rows_inserted=total_inserted,
            duplicates_skipped=len(records) - total_inserted,
        )
        
        return len(records), total_inserted
    
    def get_latest_timestamp(
        self,
        symbol: str,
        source: Optional[DataSource] = None,
    ) -> Optional[datetime]:
        """
        Get the most recent timestamp for a symbol.
        
        Useful for incremental ingestion - only fetch data newer than this.
        
        Args:
            symbol: Ticker symbol.
            source: Optional source filter.
            
        Returns:
            Latest timestamp or None if no data exists.
        """
        query = """
            SELECT MAX(timestamp) as latest
            FROM market_data
            WHERE symbol = %s
        """
        params = [symbol]
        
        if source:
            query += " AND source = %s"
            params.append(source.value)
        
        with get_db_cursor() as cur:
            cur.execute(query, params)
            result = cur.fetchone()
            return result[0] if result and result[0] else None
    
    def get_ohlcv(
        self,
        symbol: str,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        source: Optional[DataSource] = None,
        limit: int = 1000,
    ) -> list[dict]:
        """
        Query OHLCV data with optional filters.
        
        Args:
            symbol: Ticker symbol.
            start_date: Start of date range (inclusive).
            end_date: End of date range (inclusive).
            source: Optional source filter.
            limit: Maximum rows to return.
            
        Returns:
            List of dictionaries with OHLCV data.
        """
        query = """
            SELECT timestamp, symbol, source, open, high, low, close, volume, adjusted_close
            FROM market_data
            WHERE symbol = %s
        """
        params: list = [symbol]
        
        if start_date:
            query += " AND timestamp >= %s"
            params.append(start_date)
        
        if end_date:
            query += " AND timestamp <= %s"
            params.append(end_date)
        
        if source:
            query += " AND source = %s"
            params.append(source.value)
        
        query += " ORDER BY timestamp DESC LIMIT %s"
        params.append(limit)
        
        with get_db_cursor(dict_cursor=True) as cur:
            cur.execute(query, params)
            return cur.fetchall()
    
    def get_cross_source_comparison(
        self,
        symbol: str,
        days: int = 7,
    ) -> list[dict]:
        """
        Get price comparison across sources for a symbol.
        
        Useful for data quality validation - detect when sources disagree.
        
        Args:
            symbol: Ticker symbol.
            days: Number of days to look back.
            
        Returns:
            List of dictionaries with cross-source price comparisons.
        """
        query = """
            SELECT * FROM v_cross_source_comparison
            WHERE symbol = %s
            AND timestamp > NOW() - INTERVAL '%s days'
            ORDER BY timestamp DESC, pct_diff DESC
        """
        
        with get_db_cursor(dict_cursor=True) as cur:
            cur.execute(query, (symbol, days))
            return cur.fetchall()


class QualityMetricsRepository:
    """
    Repository for data quality metrics.
    
    Stores results from Great Expectations validation runs.
    """
    
    def insert_results(
        self,
        source: DataSource,
        suite_name: str,
        results: list[QualityCheckResult],
    ) -> int:
        """
        Insert quality check results.
        
        Args:
            source: Data source that was validated.
            suite_name: Name of the Great Expectations suite.
            results: List of individual check results.
            
        Returns:
            Number of rows inserted.
        """
        if not results:
            return 0
        
        values = [
            (
                source.value,
                suite_name,
                r.expectation_type,
                r.column_name,
                r.success,
                r.observed_value,
                r.expected_value,
                psycopg2.extras.Json(r.details),
            )
            for r in results
        ]
        
        insert_sql = """
            INSERT INTO quality_metrics
            (source, suite_name, expectation_type, column_name, success, 
             observed_value, expected_value, details)
            VALUES %s
        """
        
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                execute_values(cur, insert_sql, values)
                rows = cur.rowcount
                conn.commit()
        
        return rows
    
    def get_quality_summary(
        self,
        days: int = 7,
        source: Optional[DataSource] = None,
    ) -> list[dict]:
        """
        Get quality summary for dashboard.
        
        Args:
            days: Number of days to look back.
            source: Optional source filter.
            
        Returns:
            List of daily quality summaries.
        """
        query = """
            SELECT * FROM v_quality_summary
            WHERE date > NOW() - INTERVAL '%s days'
        """
        params: list = [days]
        
        if source:
            query += " AND source = %s"
            params.append(source.value)
        
        query += " ORDER BY date DESC, source"
        
        with get_db_cursor(dict_cursor=True) as cur:
            cur.execute(query, params)
            return cur.fetchall()
    
    def get_recent_failures(self, limit: int = 20) -> list[dict]:
        """
        Get recent quality check failures for investigation.
        
        Args:
            limit: Maximum number of failures to return.
            
        Returns:
            List of failed quality checks with details.
        """
        query = """
            SELECT run_timestamp, source, suite_name, expectation_type,
                   column_name, observed_value, expected_value, details
            FROM quality_metrics
            WHERE success = FALSE
            ORDER BY run_timestamp DESC
            LIMIT %s
        """
        
        with get_db_cursor(dict_cursor=True) as cur:
            cur.execute(query, (limit,))
            return cur.fetchall()


class IngestionLogRepository:
    """
    Repository for ingestion run logs.
    """
    
    def log_run(
        self,
        source: DataSource,
        symbol: str,
        status: str,
        rows_fetched: int = 0,
        rows_inserted: int = 0,
        rows_updated: int = 0,
        duration_ms: int = 0,
        error_message: Optional[str] = None,
        metadata: Optional[dict] = None,
    ) -> int:
        """
        Log an ingestion run.
        
        Args:
            source: Data source.
            symbol: Symbol that was ingested.
            status: 'success', 'partial', or 'failed'.
            rows_fetched: Number of rows from API.
            rows_inserted: Number of new rows inserted.
            rows_updated: Number of rows updated (if using DO UPDATE).
            duration_ms: Time taken in milliseconds.
            error_message: Error details if failed.
            metadata: Additional metadata as JSON.
            
        Returns:
            ID of the log entry.
        """
        insert_sql = """
            INSERT INTO ingestion_logs
            (source, symbol, status, rows_fetched, rows_inserted, 
             rows_updated, duration_ms, error_message, metadata)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        """
        
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    insert_sql,
                    (
                        source.value,
                        symbol,
                        status,
                        rows_fetched,
                        rows_inserted,
                        rows_updated,
                        duration_ms,
                        error_message,
                        psycopg2.extras.Json(metadata or {}),
                    ),
                )
                result = cur.fetchone()
                conn.commit()
                return result[0]


# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================

def check_database_health() -> bool:
    """
    Check if database is reachable and healthy.
    
    Returns:
        True if database is healthy, False otherwise.
    """
    try:
        with get_db_cursor() as cur:
            cur.execute("SELECT 1")
            return True
    except Exception as e:
        logger.error("database_health_check_failed", error=str(e))
        return False


def get_table_stats() -> dict:
    """
    Get basic statistics about database tables.
    
    Returns:
        Dictionary with row counts and date ranges.
    """
    with get_db_cursor(dict_cursor=True) as cur:
        cur.execute("""
            SELECT 
                COUNT(*) as total_rows,
                COUNT(DISTINCT symbol) as unique_symbols,
                COUNT(DISTINCT source) as unique_sources,
                MIN(timestamp) as earliest_date,
                MAX(timestamp) as latest_date
            FROM market_data
        """)
        market_data_stats = cur.fetchone()
        
        cur.execute("SELECT COUNT(*) as total_checks FROM quality_metrics")
        quality_stats = cur.fetchone()
        
        return {
            "market_data": dict(market_data_stats) if market_data_stats else {},
            "quality_metrics": dict(quality_stats) if quality_stats else {},
        }
