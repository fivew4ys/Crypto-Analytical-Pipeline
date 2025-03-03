from pydantic import BaseModel, Field, field_validator
from typing import List, Optional, Literal

class AppConfig(BaseModel):
    """Application configuration model."""
    asset_type: Literal["spot", "um", "cm"] = Field(..., description="Asset type: spot, um (USD-M Futures), cm (COIN-M Futures)")
    time_period: Literal["daily", "monthly"] = Field(..., description="Time period: daily or monthly")
    data_type: str = Field(..., description="Data type: klines, trades, etc.")
    data_frequency: str = Field(..., description="Data frequency: 1m, 1h, 1d, etc.")
    destination_dir: str = Field("./binance_data", description="Directory to save downloaded data")
    max_workers: int = Field(50, description="Max workers for downloading")
    max_extract_workers: int = Field(10, description="Max workers for extraction")
    symbol_suffix: Optional[List[str]] = Field(None, description="Filter symbols by suffix (e.g., USDT)")
    batch_number: int = Field(1, description="Current batch number")
    total_batches: int = Field(1, description="Total number of batches")
    retries: int = Field(3, description="Number of retries for requests")
    
    @field_validator('asset_type')
    def validate_asset_type(cls, v):
        if v not in ["spot", "um", "cm"]:
            raise ValueError("asset_type must be one of 'spot', 'um', 'cm'")
        return v

    @field_validator('time_period')
    def validate_time_period(cls, v):
        if v not in ["daily", "monthly"]:
            raise ValueError("time_period must be one of 'daily', 'monthly'")
        return v
