import argparse
from rich.console import Console
from config import AppConfig
from pipeline import Pipeline

__version__ = "0.5.0"

def parse_args():
    parser = argparse.ArgumentParser(description="Binance Data Downloader")
    parser.add_argument("--asset-type", choices=["spot", "um", "cm", "option"], default="spot", help="Asset type")
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
    parser.add_argument("--fetch-method", choices=["api", "xml", "json"], default="api", help="Method to fetch symbols: api (default), xml (for Colab), or json")
    parser.add_argument("--symbol-file", help="Path to JSON file containing symbols (required if fetch-method is json)")
    parser.add_argument("--db-path", help="Path to DuckDB database file (optional)")
    parser.add_argument("--config", help="Path to YAML configuration file")
    return parser.parse_args()

def main():
    """Main execution flow."""
    console = Console()
    args = parse_args()
    
    try:
        if args.config:
            # Load from YAML if provided
            pipeline = Pipeline(args.config)
        else:
            # Load from CLI args
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
                retries=args.retries,
                fetch_method=args.fetch_method,
                symbol_file=args.symbol_file,
                db_path=args.db_path
            )
            pipeline = Pipeline(config)
            
        pipeline.run()
        
    except Exception as e:
        console.print(f"[bold red]Error: {e}[/]")

if __name__ == "__main__":
    main()