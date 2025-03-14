import argparse
import sys
import os
from rich.console import Console
from rich.progress import Progress
from concurrent.futures import ThreadPoolExecutor, as_completed

from config import AppConfig
from api_symbol_fetcher import ApiSymbolFetcher
from downloader import Downloader
from extractor import Extractor
from verifier import Verifier

__version__ = "0.3.0"

def parse_args():
    parser = argparse.ArgumentParser(description="Binance Data Downloader")
    parser.add_argument("--asset-type", choices=["spot", "um", "cm"], default="spot", help="Asset type")
    parser.add_argument("--time-period", choices=["daily", "monthly"], default="monthly", help="Time period")
    parser.add_argument("--data-type", default="klines", help="Data type (e.g., klines, trades)")
    parser.add_argument("--data-frequency", default="1m", help="Data frequency (e.g., 1m, 1h)")
    parser.add_argument("--destination-dir", default="./binance_data", help="Destination directory")
    parser.add_argument("--max-workers", type=int, default=50, help="Max download workers")
    parser.add_argument("--max-extract-workers", type=int, default=10, help="Max extraction workers")
    parser.add_argument("--symbol-suffix", nargs="+", default=["USDT"], help="Filter symbols by suffix")
    parser.add_argument("--batch-number", type=int, default=1, help="Batch number")
    parser.add_argument("--total-batches", type=int, default=1, help="Total batches")
    parser.add_argument("--retries", type=int, default=3, help="Number of retries")
    return parser.parse_args()

def main():
    """Main execution flow."""
    console = Console()
    args = parse_args()
    
    # Configuration
    try:
        config = AppConfig(
            asset_type=args.asset_type,
            time_period=args.time_period,
            data_type=args.data_type,
            data_frequency=args.data_frequency,
            destination_dir=args.destination_dir,
            max_workers=args.max_workers,
            max_extract_workers=args.max_extract_workers,
            symbol_suffix=args.symbol_suffix,
            batch_number=args.batch_number,
            total_batches=args.total_batches,
            retries=args.retries
        )
    except Exception as e:
        console.print(f"[bold red]Configuration error: {e}[/]")
        return

    # Initialize components
    # Use ApiSymbolFetcher instead of SymbolFetcher
    fetcher = ApiSymbolFetcher()
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