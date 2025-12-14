
import pytest
from pydantic import ValidationError
from crypto_pipeline.config import AppConfig

def test_option_monthly_fails():
    """Test that option asset type with monthly time period fails validation."""
    with pytest.raises(ValidationError) as excinfo:
        AppConfig(
            asset_type="option",
            time_period="monthly",
            data_type="klines",
            data_frequency="1d"
        )
    assert "Option data is only available for 'daily' time period" in str(excinfo.value)

def test_option_daily_passes():
    """Test that option asset type with daily time period passes."""
    config = AppConfig(
        asset_type="option",
        time_period="daily",
        data_type="klines",
        data_frequency="1d"
    )
    assert config.asset_type == "option"
    assert config.time_period == "daily"

def test_spot_monthly_passes():
    """Test that spot asset type with monthly time period passes."""
    config = AppConfig(
        asset_type="spot",
        time_period="monthly",
        data_type="klines",
        data_frequency="1d"
    )
    assert config.asset_type == "spot"
    assert config.time_period == "monthly"
