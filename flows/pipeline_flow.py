from prefect import flow, task
from rich.console import Console
from typing import List
import os

from src.config import AppConfig
from src.symbol_fetcher import SymbolFetcher
from src.downloader import Downloader
from src.extractor import Extractor
from src.verifier import Verifier
from src.loader import DuckDBLoader
from src.schema_monitor import SchemaMonitor

# Define Tasks
@task(name="Check Schema")
def check_schema_task(config: AppConfig) -> bool:
    return SchemaMonitor().check_schema(config)

@task(name="Fetch Symbols", retries=3)
def fetch_symbols_task(config: AppConfig) -> List[str]:
    return SymbolFetcher().get_symbols(config)

@task(name="Download Batch")
def download_batch_task(symbols: List[str], config: AppConfig) -> List[str]:
    downloader = Downloader()
    return downloader.download(symbols, config)

@task(name="Extract Files")
def extract_task(download_urls: List[str], config: AppConfig):
    # We need to replicate the extraction logic here or call the extractor
    # Since extractor.extract takes content, and downloader returns URLs, 
    # we might need to adjust how we call this to match the Pipeline logic.
    # For simplicity in this flow, we'll assume the Downloader handles the heavy lifting 
    # or we re-implement the concurrent download/extract logic inside a task.
    
    # Actually, the Pipeline class has a complex concurrent download+extract block.
    # To preserve that performance, we might want to wrap that whole block as a task.
    
    # Let's instantiate the components
    downloader = Downloader()
    extractor = Extractor()
    
    # For the sake of the Prefect example, we will do a simplified sequential or 
    # internal-concurrent version. Re-using the logic from Pipeline would be best.
    # But we can't easily import the logic block from Pipeline.run().
    
    # Let's just call the downloader's download method which gets URLs, 
    # then we need to actually download the files and extract them.
    # The current Downloader.download ONLY returns URLs.
    
    # We will implement the download+extract loop here.
    from concurrent.futures import ThreadPoolExecutor, as_completed
    
    console = Console()
    
    with ThreadPoolExecutor(max_workers=config.max_workers) as dl_executor, \
         ThreadPoolExecutor(max_workers=config.max_extract_workers) as ex_executor:
        
        def process_download(url):
            parts = url.split('/')
            if config.asset_type == "spot":
                symbol = parts[7]
            elif config.asset_type == "option":
                symbol = parts[7]
            else:
                symbol = parts[8]
            final_path = os.path.join(config.destination_dir, config.asset_type, symbol, config.data_frequency)
            os.makedirs(final_path, exist_ok=True)
            
            try:
                content = downloader.download_file(url, final_path, config)
                ex_executor.submit(extractor.extract, content, final_path, config)
            except Exception:
                pass

        futures = [dl_executor.submit(process_download, url) for url in download_urls]
        for _ in as_completed(futures):
            pass

@task(name="Verify Data")
def verify_task(symbols: List[str], config: AppConfig):
    Verifier().verify(symbols, config)

@task(name="Load to DuckDB")
def load_task(symbols: List[str], config: AppConfig):
    DuckDBLoader().load(symbols, config)

# Define Flow
@flow(name="Crypto Data Pipeline")
def crypto_pipeline_flow(config_path: str = None, asset_type: str = "spot"):
    console = Console()
    
    # Load Config
    if config_path:
        config = AppConfig.from_yaml(config_path)
    else:
        # Default config if none provided
        config = AppConfig(
            asset_type=asset_type,
            time_period="monthly",
            data_type="klines",
            data_frequency="1m",
            destination_dir="./binance_data",
            fetch_method="api"
        )
    
    # 0. Schema Check
    if not check_schema_task(config):
        console.print("Schema check failed. Aborting.")
        return

    # 1. Fetch
    symbols = fetch_symbols_task(config)
    if not symbols:
        console.print("No symbols found.")
        return

    # Batching (Simplified for Flow: just process batch 1 or all?)
    # Let's process the batch defined in config
    batch_size_total = len(symbols) // config.total_batches
    remainder = len(symbols) % config.total_batches
    batches = []
    start = 0
    for i in range(config.total_batches):
        end = start + batch_size_total + (1 if i < remainder else 0)
        batches.append(symbols[start:end])
        start = end

    current_batch = batches[config.batch_number-1]
    console.print(f"Processing batch {config.batch_number}/{config.total_batches} ({len(current_batch)} symbols)")

    # 2. Download (Get URLs)
    urls = download_batch_task(current_batch, config)
    
    # 3. Extract (Download Content & Extract)
    extract_task(urls, config)
    
    # 4. Verify
    verify_task(current_batch, config)
    
    # 5. Load
    load_task(current_batch, config)

if __name__ == "__main__":
    # Example run
    crypto_pipeline_flow()
