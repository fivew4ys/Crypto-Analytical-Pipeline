import os
from rich.console import Console
from rich.progress import Progress
from concurrent.futures import ThreadPoolExecutor, as_completed
from natsort import natsorted

from config import AppConfig
from symbol_fetcher import SymbolFetcher
from downloader import Downloader
from extractor import Extractor
from verifier import Verifier

__version__ = "0.2.0"

def main():
    """Main execution flow."""
    console = Console()
    
    # Configuration
    try:
        config = AppConfig(
            asset_type="spot",
            time_period="monthly",
            data_type="klines",
            data_frequency="1m",
            destination_dir="./binance_data",
            symbol_suffix=["USDT"],
            batch_number=1,
            total_batches=3,
            max_workers=50,
            max_extract_workers=10
        )
    except Exception as e:
        console.print(f"[bold red]Configuration error: {e}[/]")
        return

    # Initialize components
    fetcher = SymbolFetcher()
    downloader = Downloader()
    extractor = Extractor()
    verifier = Verifier()

    # Create directory
    try:
        os.makedirs(config.destination_dir, exist_ok=True)
    except Exception as e:
        console.print(f"[bold red]Directory error: {e}[/]")
        return

    # 1. Fetch Symbols
    symbols = fetcher.get_symbols(config)
    if not symbols:
        console.print("[bold red]No symbols found[/]")
        return

    # Batching
    batch_size_total = len(symbols) // config.total_batches
    remainder = len(symbols) % config.total_batches
    batches = []
    start = 0
    for i in range(config.total_batches):
        end = start + batch_size_total + (1 if i < remainder else 0)
        batches.append(symbols[start:end])
        start = end

    current_batch = batches[config.batch_number-1]
    console.print(f"\n[bold green]Processing batch {config.batch_number}/{config.total_batches} ({len(current_batch)} symbols)[/]")

    # 2. Download
    download_urls = downloader.download(current_batch, config)
    
    # 3. Download & Extract Execution
    with Progress() as progress:
        dl_task = progress.add_task("[cyan]Downloading...", total=len(download_urls))
        ex_task = progress.add_task("[green]Extracting...", total=len(download_urls))
        
        with ThreadPoolExecutor(max_workers=config.max_workers) as dl_executor, \
             ThreadPoolExecutor(max_workers=config.max_extract_workers) as ex_executor:
            
            futures = []
            for url in download_urls:
                # Determine destination path logic here or in downloader/extractor?
                # Keeping logic similar to original for now
                parts = url.split('/')
                if config.asset_type == "spot":
                    symbol = parts[7]
                else:
                    symbol = parts[8]
                
                final_path = os.path.join(config.destination_dir, config.asset_type, symbol, config.data_frequency)
                os.makedirs(final_path, exist_ok=True)

                futures.append(dl_executor.submit(
                    downloader.download_file, url, final_path, config
                ))
                
                # We need to handle the extraction callback. 
                # The original code submitted extraction after download.
                # Let's adapt:
            
            # Refined approach for chaining:
            # We can't easily chain with separate executors in this loop structure without callbacks.
            # Let's use the original callback style but with our objects.
            
            # Re-creating futures list for proper handling
            pass

    # Redoing the execution loop to match the requirement of chaining download -> extract
    with Progress() as progress:
        dl_task = progress.add_task("[cyan]Downloading...", total=len(download_urls))
        ex_task = progress.add_task("[green]Extracting...", total=len(download_urls))
        
        with ThreadPoolExecutor(max_workers=config.max_workers) as dl_executor, \
             ThreadPoolExecutor(max_workers=config.max_extract_workers) as ex_executor:
            
            def process_download(url):
                parts = url.split('/')
                if config.asset_type == "spot":
                    symbol = parts[7]
                else:
                    symbol = parts[8]
                final_path = os.path.join(config.destination_dir, config.asset_type, symbol, config.data_frequency)
                os.makedirs(final_path, exist_ok=True)
                
                try:
                    content = downloader.download_file(url, final_path, config)
                    ex_executor.submit(extractor.extract, content, final_path, config).add_done_callback(
                        lambda _: progress.advance(ex_task)
                    )
                    progress.advance(dl_task)
                except Exception:
                    pass # Error handled in downloader

            futures = [dl_executor.submit(process_download, url) for url in download_urls]
            for _ in as_completed(futures):
                pass

    # 4. Verify
    verifier.verify(current_batch, config)
    console.print("[bold green]\nProcess completed[/]")

if __name__ == "__main__":
    main()