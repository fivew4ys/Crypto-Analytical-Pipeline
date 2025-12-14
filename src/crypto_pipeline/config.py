from pydantic import BaseModel, Field, field_validator, model_validator
from typing import List, Optional, Literal

class AppConfig(BaseModel):
    """Application configuration model."""
    asset_type: Literal["spot", "um", "cm", "option"] = Field(..., description="Asset type: spot, um, cm, option")
    time_period: Literal["daily", "monthly"] = Field(..., description="Time period: daily or monthly")
    data_type: str = Field(..., description="Data type: klines, trades, etc.")
    data_frequency: Optional[str] = Field(None, description="Data frequency: 1m, 1h, 1d, etc. (Optional for trades)")
    destination_dir: str = Field("./binance_data", description="Directory to save downloaded data")
    max_workers: int = Field(50, description="Max workers for downloading")
    max_extract_workers: int = Field(10, description="Max workers for extraction")
    symbol_suffix: Optional[List[str]] = Field(None, description="Filter symbols by suffix (e.g., USDT)")
    batch_number: int = Field(1, description="Current batch number")
    total_batches: int = Field(1, description="Total number of batches")
    retries: int = Field(3, description="Number of retries for requests")
    fetch_method: Literal["api", "xml", "json"] = Field("api", description="Method to fetch symbols: api, xml, or json")
    symbol_file: Optional[str] = Field(None, description="Path to JSON file containing symbols (required if fetch_method is json)")
    db_path: Optional[str] = Field(None, description="Path to DuckDB database file (optional)")
    
    @field_validator('asset_type')
    def validate_asset_type(cls, v):
        if v not in ["spot", "um", "cm", "option"]:
            raise ValueError("asset_type must be one of 'spot', 'um', 'cm', 'option'")
        return v

    @field_validator('time_period')
    def validate_time_period(cls, v, info):
        if v not in ["daily", "monthly"]:
            raise ValueError("time_period must be one of 'daily', 'monthly'")
        
        # Check if asset_type is available in validation info
        if info.data.get('asset_type') == 'option' and v == 'monthly':
             raise ValueError("Option data is only available for 'daily' time period.")
        return v
        
    @model_validator(mode='after')
    def check_frequency_requirement(self):
        if 'klines' in self.data_type.lower() and not self.data_frequency:
             raise ValueError(f"data_frequency is required for {self.data_type} data type.")
        return self

    @classmethod
    def from_yaml(cls, path: str):
        """Load configuration from a YAML file."""
        import yaml
        with open(path, 'r') as f:
            data = yaml.safe_load(f)
        return cls(**data)
