"""
Data Ingestion - Base Classes and Utilities

This module provides the foundation for all data source integrations.
Each data source (Yahoo Finance, Alpha Vantage, Polygon) implements
the BaseDataSource abstract class.

Design patterns used:
1. Strategy Pattern: Interchangeable data sources with common interface
2. Template Method: Common workflow with source-specific fetch logic
3. Retry with Exponential Backoff: Handle transient API failures gracefully
"""

import logging
import os
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Optional

import structlog
import time
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)
import yfinance as yf
from alpha_vantage.timeseries import TimeSeries
import pandas as pd
from decimal import Decimal
from src.models import DataSource, OHLCVRecord


# Configure structured logging
structlog.configure(
    processors=[
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.JSONRenderer(),
    ],
    wrapper_class=structlog.stdlib.BoundLogger,
    context_class=dict,
    logger_factory=structlog.stdlib.LoggerFactory(),
    cache_logger_on_first_use=True,
)

logger = structlog.get_logger(__name__)


class DataSourceError(Exception):
    """Base exception for data source errors."""
    pass


class RateLimitError(DataSourceError):
    """Raised when API rate limit is hit."""
    pass


class DataFetchError(DataSourceError):
    """Raised when data fetch fails."""
    pass


class BaseDataSource(ABC):
    """
    Abstract base class for all data sources.
    
    Implement this interface to add new data sources. Each implementation
    must provide:
    1. source_name: Identifier for tracking data provenance
    2. fetch_ohlcv: Core method to retrieve market data
    3. Optionally override _validate_response for source-specific validation
    
    Example usage:
        source = YahooFinanceSource()
        records = source.fetch_ohlcv("AAPL", start_date, end_date)
    """
    
    def __init__(self, api_key: Optional[str] = None):
        """
        Initialize data source.
        
        Args:
            api_key: Optional API key (required for some sources)
        """
        self.api_key = api_key
        self._logger = structlog.get_logger(self.__class__.__name__)
    
    @property
    @abstractmethod
    def source_name(self) -> DataSource:
        """Return the data source identifier."""
        pass
    
    @abstractmethod
    def fetch_ohlcv(
        self,
        symbol: str,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
    ) -> list[OHLCVRecord]:
        """
        Fetch OHLCV data for a symbol.
        
        Args:
            symbol: Ticker symbol (e.g., "AAPL")
            start_date: Start of date range (inclusive). None = source default.
            end_date: End of date range (inclusive). None = today.
            
        Returns:
            List of OHLCVRecord objects, sorted by timestamp ascending.
            
        Raises:
            DataFetchError: If fetch fails after retries
            RateLimitError: If rate limit is exceeded
        """
        pass
    
    def _validate_response(self, data: list[dict]) -> list[dict]:
        """
        Validate API response data.
        
        Override in subclasses for source-specific validation.
        Default implementation returns data unchanged.
        
        Args:
            data: Raw data from API
            
        Returns:
            Validated data
        """
        return data
    
    def _log_fetch_start(self, symbol: str, start_date: datetime, end_date: datetime) -> None:
        """Log the start of a fetch operation."""
        self._logger.info(
            "fetch_started",
            symbol=symbol,
            source=self.source_name.value,
            start_date=start_date.isoformat() if start_date else None,
            end_date=end_date.isoformat() if end_date else None,
        )
    
    def _log_fetch_complete(
        self, symbol: str, record_count: int, duration_ms: int
    ) -> None:
        """Log successful fetch completion."""
        self._logger.info(
            "fetch_completed",
            symbol=symbol,
            source=self.source_name.value,
            record_count=record_count,
            duration_ms=duration_ms,
        )
    
    def _log_fetch_error(self, symbol: str, error: Exception) -> None:
        """Log fetch error."""
        self._logger.error(
            "fetch_failed",
            symbol=symbol,
            source=self.source_name.value,
            error_type=type(error).__name__,
            error_message=str(error),
        )


def with_retry(func):
    """
    Decorator for retry logic with exponential backoff.
    
    Retry strategy:
    - Max 3 attempts
    - Wait 1, 2, 4 seconds between retries (exponential)
    - Only retry on transient errors (network, rate limit)
    
    Why exponential backoff?
    - Prevents thundering herd when multiple workers hit rate limits
    - Gives external services time to recover
    - More polite to API providers than immediate retries
    """
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type((RateLimitError, ConnectionError, TimeoutError)),
        reraise=True,
    )
    def wrapper(*args, **kwargs):
        return func(*args, **kwargs)
    
    return wrapper


# =============================================================================
# STUB IMPLEMENTATIONS - YOU WILL IMPLEMENT THESE
# =============================================================================

class YahooFinanceSource(BaseDataSource):
    """
    Yahoo Finance data source using yfinance library.
    
    Pros:
    - No API key required
    - Good historical data coverage
    - Free and reliable
    
    Cons:
    - Unofficial API (could break)
    - Rate limiting is implicit
    - Adjusted close may differ from other sources
    
    TODO: Implement in Session 1
    """
    
    @property
    def source_name(self) -> DataSource:
        return DataSource.YAHOO_FINANCE
    
    @with_retry
    def fetch_ohlcv(
        self,
        symbol: str,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
    ) -> list[OHLCVRecord]:
        """
        Fetch OHLCV data from Yahoo Finance.

        Implementation hints:
        1. Use yfinance.Ticker(symbol).history(start=..., end=...)
        2. DataFrame columns: Open, High, Low, Close, Volume
        3. Index is DatetimeIndex (already timezone-aware)
        4. Convert each row to OHLCVRecord
        5. Handle empty responses gracefully
        """
        symbol = symbol.upper()

        # Log fetch start
        start_time = time.time()
        self._log_fetch_start(symbol, start_date, end_date)

        try:
            ohlcv_list: list[OHLCVRecord] = []

            ticker = yf.Ticker(symbol)

            # Build parameters for history call
            params = {}
            if start_date is None and end_date is None:
                params["period"] = "1mo"
            else:
                if start_date is not None:
                    params["start"] = start_date.strftime("%Y-%m-%d")
                if end_date is not None:
                    params["end"] = end_date.strftime("%Y-%m-%d")

            df = ticker.history(**params)

            # Check if empty response
            if df.empty:
                self._logger.warning(
                    "empty_response",
                    symbol=symbol,
                    source=self.source_name.value
                )
                return ohlcv_list

            # Cast to Decimal for precision
            for column in ["Open", "High", "Low", "Close", "Volume"]:
                df[column] = df[column].apply(lambda x: Decimal(str(x)))

            # Convert timestamp from NY time to timezone naive UTC
            if df.index.tzinfo is not None:
                df.index = df.index.tz_convert("UTC").tz_localize(None)

            # Convert to OHLCVRecord objects
            for index, row in df.iterrows():
                record = OHLCVRecord(
                    timestamp=index.to_pydatetime(),
                    symbol=symbol,
                    source=DataSource.YAHOO_FINANCE,
                    open=row["Open"],
                    high=row["High"],
                    low=row["Low"],
                    close=row["Close"],
                    volume=int(row["Volume"]),
                    adjusted_close=None,
                    metadata={
                        "dividends": float(row["Dividends"]),
                        "stock_splits": float(row["Stock Splits"])
                    }
                )
                ohlcv_list.append(record)

            # Log fetch complete
            duration_ms = int((time.time() - start_time) * 1000)
            self._log_fetch_complete(symbol, len(ohlcv_list), duration_ms)

            return ohlcv_list

        except Exception as e:
            # Log fetch error
            self._log_fetch_error(symbol, e)
            raise DataFetchError(f"Failed to fetch data for {symbol}: {e}") from e


class AlphaVantageSource(BaseDataSource):
    """
    Alpha Vantage data source.
    
    Pros:
    - Reliable API with good documentation
    - Adjusted close with dividend/split handling
    - Supports multiple data types (forex, crypto, etc.)
    
    Cons:
    - Free tier: 5 calls/minute, 500 calls/day
    - Requires API key
    
    TODO: Implement in Session 1
    """
    
    def __init__(self, api_key: str):
        super().__init__(api_key)
        if not api_key:
            raise ValueError("Alpha Vantage requires an API key")
    
    @property
    def source_name(self) -> DataSource:
        return DataSource.ALPHA_VANTAGE
    
    def fetch_ohlcv(
        self,
        symbol: str,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
    ) -> list[OHLCVRecord]:
        """
        Fetch OHLCV data from Alpha Vantage.
        
        Implementation hints:
        1. Use TIME_SERIES_DAILY_ADJUSTED endpoint
        2. API returns JSON with "Time Series (Daily)" key
        3. Each day has: "1. open", "2. high", etc.
        4. Filter by date range after fetching
        5. Handle rate limiting (wait and retry)
        """
        ts = TimeSeries(key=os.getenv('ALPHA_VANTAGE_API_KEY'), output_format='pandas')

        ts = TimeSeries(key=os.getenv('ALPHA_VANTAGE_API_KEY'), output_format='pandas')
        df: pd.DataFrame
        df, meta_data = ts.get_daily(symbol=symbol)

        data = data.rename(columns={
            'open':'Open',
            'high':'High',
            'low':'Low',
            'close': 'Close',
            'volume': 'Volume'
        })

        ohlcv_list: list[OHLCVRecord] = []
        for index, row in data.iterrows():
            record = OHLCVRecord(
                timestamp = index.to_pydatetime(),
                symbol=symbol,
                source=DataSource.YAHOO_FINANCE,
                open=row["Open"],
                high=row["High"],
                low=row["Low"],
                close=row["Close"],
                volume=int(row["Volume"]),
                adjusted_close=None,
                metadata={}
            )
            ohlcv_list.append(record)

        return ohlcv_list


class PolygonSource(BaseDataSource):
    """
    Polygon.io data source.
    
    Pros:
    - Professional-grade data quality
    - Real-time data available (paid tiers)
    - Excellent API design
    
    Cons:
    - Free tier has limitations
    - Requires API key
    
    TODO: Optional - implement if time permits
    """
    
    def __init__(self, api_key: str):
        super().__init__(api_key)
        if not api_key:
            raise ValueError("Polygon requires an API key")
    
    @property
    def source_name(self) -> DataSource:
        return DataSource.POLYGON
    
    def fetch_ohlcv(
        self,
        symbol: str,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
    ) -> list[OHLCVRecord]:
        """
        Fetch OHLCV data from Polygon.io.
        
        Implementation hints:
        1. Use /v2/aggs/ticker/{ticker}/range/1/day/{from}/{to}
        2. Response has "results" array with bars
        3. Each bar: o, h, l, c, v, t (timestamp in ms)
        4. Handle pagination for large date ranges
        """
        # TODO: Optional implementation
        raise NotImplementedError("Implement Polygon ingestion")


# =============================================================================
# DATA SOURCE FACTORY
# =============================================================================

def get_data_source(
    source: DataSource,
    api_key: Optional[str] = None,
) -> BaseDataSource:
    """
    Factory function to create data source instances.
    
    This pattern allows easy addition of new sources without
    modifying calling code.
    
    Args:
        source: Which data source to create
        api_key: API key if required
        
    Returns:
        Configured data source instance
        
    Raises:
        ValueError: If source is not supported
    """
    sources = {
        DataSource.YAHOO_FINANCE: lambda: YahooFinanceSource(),
        DataSource.ALPHA_VANTAGE: lambda: AlphaVantageSource(api_key),
        DataSource.POLYGON: lambda: PolygonSource(api_key),
    }
    
    if source not in sources:
        raise ValueError(f"Unsupported data source: {source}")
    
    return sources[source]()
