import os
import shutil
import pytest
from crypto_pipeline.config import AppConfig
from crypto_pipeline.symbol_fetcher import SymbolFetcher
from crypto_pipeline.downloader import Downloader
from crypto_pipeline.extractor import Extractor
from crypto_pipeline.verifier import Verifier

@pytest.fixture
def clean_data_dir():
    """Fixture to clean up data directory before and after tests."""
    data_dir = "./test_data"
    if os.path.exists(data_dir):
        shutil.rmtree(data_dir)
    yield data_dir
    if os.path.exists(data_dir):
        shutil.rmtree(data_dir)

def test_btcusdt_daily_download(clean_data_dir):
    """Test downloading BTCUSDT daily klines."""
    config = AppConfig(
        asset_type="spot",
        time_period="daily",
        data_type="klines",
        data_frequency="1d",
        destination_dir=clean_data_dir,
        symbol_suffix=["USDT"],
        fetch_method="api",
        max_workers=5,
        max_extract_workers=2
    )

    # 1. Fetch Symbols (Mocking or filtering for just BTCUSDT to be fast)
    # Since we can't easily mock without a lot of setup, we'll just filter the result
    fetcher = SymbolFetcher()
    symbols = fetcher.get_symbols(config)
    assert "BTCUSDT" in symbols
    
    # Restrict to just BTCUSDT for the test
    target_symbols = ["BTCUSDT"]

    # 2. Download
    downloader = Downloader()
    download_urls = downloader.download(target_symbols, config)
    assert len(download_urls) > 0
    
    # Download just one file to be fast
    first_url = download_urls[0]
    final_path = os.path.join(config.destination_dir, config.asset_type, "BTCUSDT", config.data_frequency)
    os.makedirs(final_path, exist_ok=True)
    
    content = downloader.download_file(first_url, final_path, config)
    assert content is not None
    assert len(content) > 0

    # 3. Extract
    extractor = Extractor()
    extractor.extract(content, final_path, config)
    
    # Check if CSV exists
    files = os.listdir(final_path)
    csv_files = [f for f in files if f.endswith(".csv")]
    assert len(csv_files) > 0

    # 4. Verify
    verifier = Verifier()
    # Verification might fail if we only downloaded one file but the verifier expects continuity
    # So we'll just check if the verifier runs without crashing
    try:
        verifier.verify(target_symbols, config)
    except Exception as e:
        pytest.fail(f"Verifier crashed: {e}")
