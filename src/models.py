"""
Pydantic Models for Financial Data Pipeline

These models serve three purposes:
1. Runtime validation of data from external APIs
2. Type hints for IDE support and documentation
3. Automatic API documentation in FastAPI

Design principle: Fail fast with clear errors rather than silently accepting bad data.
"""

from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field, field_validator, model_validator


class DataSource(str, Enum):
    """Supported data sources for market data."""
    YAHOO_FINANCE = "yahoo_finance"
    ALPHA_VANTAGE = "alpha_vantage"
    POLYGON = "polygon"


class OHLCVRecord(BaseModel):
    """
    Single OHLCV (Open-High-Low-Close-Volume) record.
    
    This is the core data model for market data. All external API responses
    are normalized to this format before database insertion.
    
    Validation rules:
    - All prices must be positive
    - High must be >= Low
    - High must be >= Open and Close
    - Low must be <= Open and Close
    - Volume must be non-negative
    """
    
    timestamp: datetime = Field(..., description="Bar timestamp in UTC")
    symbol: str = Field(..., min_length=1, max_length=20, description="Ticker symbol")
    source: DataSource = Field(..., description="Data source identifier")
    
    open: Decimal = Field(..., gt=0, description="Opening price")
    high: Decimal = Field(..., gt=0, description="High price")
    low: Decimal = Field(..., gt=0, description="Low price")
    close: Decimal = Field(..., gt=0, description="Closing price")
    volume: int = Field(..., ge=0, description="Trading volume")
    
    adjusted_close: Optional[Decimal] = Field(None, gt=0, description="Adjusted closing price")
    metadata: dict = Field(default_factory=dict, description="Additional source-specific data")
    
    @field_validator("symbol")
    @classmethod
    def uppercase_symbol(cls, v: str) -> str:
        """Normalize symbols to uppercase."""
        return v.upper().strip()
    
    @model_validator(mode="after")
    def validate_ohlc_relationships(self) -> "OHLCVRecord":
        """
        Validate OHLC price relationships.
        
        These relationships must hold for valid market data:
        - High >= Low (by definition)
        - High >= Open and High >= Close (high is the maximum)
        - Low <= Open and Low <= Close (low is the minimum)
        """
        if self.high < self.low:
            raise ValueError(f"High ({self.high}) cannot be less than Low ({self.low})")
        
        if self.high < self.open:
            raise ValueError(f"High ({self.high}) cannot be less than Open ({self.open})")
        
        if self.high < self.close:
            raise ValueError(f"High ({self.high}) cannot be less than Close ({self.close})")
        
        if self.low > self.open:
            raise ValueError(f"Low ({self.low}) cannot be greater than Open ({self.open})")
        
        if self.low > self.close:
            raise ValueError(f"Low ({self.low}) cannot be greater than Close ({self.close})")
        
        return self
    
    class Config:
        json_encoders = {
            Decimal: str,  # Serialize Decimals as strings to preserve precision
            datetime: lambda v: v.isoformat(),
        }


class IngestionRequest(BaseModel):
    """Request model for triggering data ingestion."""
    
    symbols: list[str] = Field(..., min_length=1, description="List of symbols to ingest")
    source: DataSource = Field(..., description="Data source to fetch from")
    start_date: Optional[datetime] = Field(None, description="Start of date range (inclusive)")
    end_date: Optional[datetime] = Field(None, description="End of date range (inclusive)")
    
    @field_validator("symbols")
    @classmethod
    def normalize_symbols(cls, v: list[str]) -> list[str]:
        """Normalize and deduplicate symbols."""
        return list(set(s.upper().strip() for s in v))
    
    @model_validator(mode="after")
    def validate_date_range(self) -> "IngestionRequest":
        """Ensure start_date <= end_date if both provided."""
        if self.start_date and self.end_date:
            if self.start_date > self.end_date:
                raise ValueError("start_date cannot be after end_date")
        return self


class IngestionResult(BaseModel):
    """Result of an ingestion operation."""
    
    symbol: str
    source: DataSource
    status: str = Field(..., pattern="^(success|partial|failed)$")
    rows_fetched: int = Field(0, ge=0)
    rows_inserted: int = Field(0, ge=0)
    rows_updated: int = Field(0, ge=0)
    duration_ms: int = Field(0, ge=0)
    error_message: Optional[str] = None


class QualityCheckResult(BaseModel):
    """Result of a single data quality check."""
    
    expectation_type: str = Field(..., description="Type of Great Expectations check")
    column_name: Optional[str] = Field(None, description="Column being checked")
    success: bool = Field(..., description="Whether the check passed")
    observed_value: Optional[str] = Field(None, description="Actual value observed")
    expected_value: Optional[str] = Field(None, description="Expected value or range")
    details: dict = Field(default_factory=dict, description="Additional check details")


class QualitySummary(BaseModel):
    """Summary of quality checks for a validation run."""
    
    run_timestamp: datetime
    source: DataSource
    suite_name: str
    total_checks: int = Field(0, ge=0)
    passed_checks: int = Field(0, ge=0)
    failed_checks: int = Field(0, ge=0)
    pass_rate: float = Field(0.0, ge=0.0, le=100.0)
    results: list[QualityCheckResult] = Field(default_factory=list)


# =============================================================================
# API Response Models
# =============================================================================

class MarketDataResponse(BaseModel):
    """API response for market data queries."""
    
    symbol: str
    source: Optional[DataSource] = None
    data: list[OHLCVRecord]
    count: int = Field(0, ge=0)


class HealthCheckResponse(BaseModel):
    """API health check response."""
    
    status: str = Field(..., pattern="^(healthy|degraded|unhealthy)$")
    database: bool
    cache: bool
    timestamp: datetime
    version: str = "1.0.0"


# =============================================================================
# Configuration Models
# =============================================================================

class DatabaseConfig(BaseModel):
    """Database connection configuration."""
    
    host: str = "localhost"
    port: int = Field(5432, ge=1, le=65535)
    database: str = "financial_data"
    user: str = "findata"
    password: str
    
    @property
    def connection_string(self) -> str:
        """Generate SQLAlchemy connection string."""
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"


class APIKeyConfig(BaseModel):
    """API key configuration for data sources."""
    
    alpha_vantage_key: Optional[str] = None
    polygon_key: Optional[str] = None
    
    @property
    def has_alpha_vantage(self) -> bool:
        return self.alpha_vantage_key is not None and len(self.alpha_vantage_key) > 0
    
    @property
    def has_polygon(self) -> bool:
        return self.polygon_key is not None and len(self.polygon_key) > 0
