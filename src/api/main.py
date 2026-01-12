"""
FastAPI Application - Market Data Query API

This module provides REST endpoints for querying market data.
Designed for production use with proper error handling, validation,
and documentation.

Endpoints:
- GET /health - Health check
- GET /market-data/{symbol} - Get OHLCV data for a symbol
- POST /ingest - Trigger data ingestion
- GET /quality/summary - Get quality metrics summary

TODO: Implement in Session 3
"""

from datetime import datetime
from typing import Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
import structlog

from src.models import (
    DataSource,
    HealthCheckResponse,
    IngestionRequest,
    IngestionResult,
    MarketDataResponse,
    QualitySummary,
)
from src.database import (
    MarketDataRepository,
    QualityMetricsRepository,
    check_database_health,
    get_table_stats,
)


logger = structlog.get_logger(__name__)

# =============================================================================
# APPLICATION SETUP
# =============================================================================

app = FastAPI(
    title="Financial Data Pipeline API",
    description="Query API for market data with quality monitoring",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
)

# CORS middleware for Streamlit dashboard
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Tighten in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Repository instances
market_data_repo = MarketDataRepository()
quality_repo = QualityMetricsRepository()


# =============================================================================
# HEALTH ENDPOINTS
# =============================================================================

@app.get("/health", response_model=HealthCheckResponse)
async def health_check():
    """
    Health check endpoint for monitoring.
    
    Returns status of database and cache connections.
    Use this for Kubernetes liveness/readiness probes.
    """
    db_healthy = check_database_health()
    
    # TODO: Add Redis health check when implementing caching
    cache_healthy = True
    
    status = "healthy" if (db_healthy and cache_healthy) else "degraded"
    
    return HealthCheckResponse(
        status=status,
        database=db_healthy,
        cache=cache_healthy,
        timestamp=datetime.utcnow(),
    )


@app.get("/stats")
async def get_stats():
    """
    Get database statistics.
    
    Useful for monitoring data volumes and date ranges.
    """
    return get_table_stats()


# =============================================================================
# MARKET DATA ENDPOINTS
# =============================================================================

@app.get("/market-data/{symbol}", response_model=MarketDataResponse)
async def get_market_data(
    symbol: str,
    start_date: Optional[datetime] = Query(None, description="Start of date range"),
    end_date: Optional[datetime] = Query(None, description="End of date range"),
    source: Optional[DataSource] = Query(None, description="Filter by source"),
    limit: int = Query(1000, ge=1, le=10000, description="Max rows to return"),
):
    """
    Get OHLCV data for a symbol.
    
    Args:
        symbol: Ticker symbol (e.g., AAPL).
        start_date: Start of date range (inclusive).
        end_date: End of date range (inclusive).
        source: Optional filter by data source.
        limit: Maximum number of rows (default 1000).
    
    Returns:
        OHLCV data sorted by timestamp descending.
    
    Example:
        GET /market-data/AAPL?start_date=2024-01-01&limit=100
    """
    # TODO: Implement - use MarketDataRepository.get_ohlcv()
    raise HTTPException(status_code=501, detail="Not implemented yet")


@app.get("/market-data/{symbol}/latest")
async def get_latest_price(symbol: str, source: Optional[DataSource] = None):
    """
    Get the most recent price for a symbol.
    
    This is optimized for low-latency access to current prices.
    Consider adding caching for production use.
    """
    # TODO: Implement
    raise HTTPException(status_code=501, detail="Not implemented yet")


@app.get("/market-data/{symbol}/cross-source")
async def get_cross_source_comparison(
    symbol: str,
    days: int = Query(7, ge=1, le=30, description="Days to look back"),
):
    """
    Get price comparison across data sources.
    
    Use this to monitor data consistency between sources.
    Returns timestamps where sources disagree beyond threshold.
    """
    # TODO: Implement - use MarketDataRepository.get_cross_source_comparison()
    raise HTTPException(status_code=501, detail="Not implemented yet")


# =============================================================================
# INGESTION ENDPOINTS
# =============================================================================

@app.post("/ingest", response_model=list[IngestionResult])
async def trigger_ingestion(request: IngestionRequest):
    """
    Trigger data ingestion for specified symbols and source.
    
    This endpoint is for manual/ad-hoc ingestion. For scheduled
    ingestion, use the orchestration layer (Prefect/Airflow).
    
    Example:
        POST /ingest
        {
            "symbols": ["AAPL", "GOOGL"],
            "source": "yahoo_finance",
            "start_date": "2024-01-01T00:00:00Z"
        }
    """
    # TODO: Implement - integrate with ingestion module
    raise HTTPException(status_code=501, detail="Not implemented yet")


# =============================================================================
# QUALITY ENDPOINTS
# =============================================================================

@app.get("/quality/summary")
async def get_quality_summary(
    days: int = Query(7, ge=1, le=30, description="Days to look back"),
    source: Optional[DataSource] = Query(None, description="Filter by source"),
):
    """
    Get quality metrics summary for dashboard.
    
    Returns daily pass/fail rates for data quality checks.
    """
    # TODO: Implement - use QualityMetricsRepository.get_quality_summary()
    raise HTTPException(status_code=501, detail="Not implemented yet")


@app.get("/quality/failures")
async def get_recent_failures(
    limit: int = Query(20, ge=1, le=100, description="Max failures to return"),
):
    """
    Get recent quality check failures.
    
    Use this to investigate and debug data quality issues.
    """
    # TODO: Implement - use QualityMetricsRepository.get_recent_failures()
    raise HTTPException(status_code=501, detail="Not implemented yet")


# =============================================================================
# STARTUP/SHUTDOWN EVENTS
# =============================================================================

@app.on_event("startup")
async def startup_event():
    """
    Run on application startup.
    
    - Verify database connection
    - Initialize any caches
    - Log startup info
    """
    logger.info("api_starting")
    
    if not check_database_health():
        logger.error("database_unavailable_at_startup")
        # Don't fail - allow health endpoint to report degraded status
    
    logger.info("api_started")


@app.on_event("shutdown")
async def shutdown_event():
    """
    Run on application shutdown.
    
    - Close database connections
    - Flush any pending metrics
    """
    logger.info("api_shutting_down")


# =============================================================================
# RUN WITH UVICORN
# =============================================================================

if __name__ == "__main__":
    import uvicorn
    
    uvicorn.run(
        "src.api.main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,  # Disable in production
    )
