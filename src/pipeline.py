from typing import Union, Dict
from rich.console import Console
from rich.progress import Progress
from concurrent.futures import ThreadPoolExecutor, as_completed
import os

from config import AppConfig
from symbol_fetcher import SymbolFetcher
from downloader import Downloader
from extractor import Extractor
from verifier import Verifier
from loader import DuckDBLoader

class Pipeline:
    """
    The main entry point for the crypto analytical pipeline.
    
    Usage:
        config = { ... }
        pipeline = Pipeline(config)
        pipeline.run()
    """
    
    def __init__(self, config: Union[AppConfig, Dict, str]):
        self.console = Console()
        
        if isinstance(config, AppConfig):
            self.config = config
        elif isinstance(config, str) and config.endswith(('.yaml', '.yml')):
            self.config = AppConfig.from_yaml(config)
        elif isinstance(config, dict):
            self.config = AppConfig(**config)
        else:
            raise ValueError("Config must be AppConfig, dict, or path to YAML file")

        self.fetcher = SymbolFetcher()
        self.downloader = Downloader()
        self.extractor = Extractor()
        self.verifier = Verifier()
        self.loader = DuckDBLoader()

    def run(self):
        """Execute the pipeline."""
        self.console.print(f"[bold green]Starting Pipeline (v0.5.0)[/]")
        self.console.print(f"Asset Type: {self.config.asset_type}")
        self.console.print(f"Time Period: {self.config.time_period}")
        
        # Create directory
        os.makedirs(self.config.destination_dir, exist_ok=True)

        # 1. Fetch Symbols
        symbols = self.fetcher.get_symbols(self.config)
        if not symbols:
            self.console.print("[bold red]No symbols found[/]")
            return

        # Batching
        batch_size_total = len(symbols) // self.config.total_batches
        remainder = len(symbols) % self.config.total_batches
        batches = []
        start = 0
        for i in range(self.config.total_batches):
            end = start + batch_size_total + (1 if i < remainder else 0)
            batches.append(symbols[start:end])
            start = end

        current_batch = batches[self.config.batch_number-1]
        self.console.print(f"\n[bold green]Processing batch {self.config.batch_number}/{self.config.total_batches} ({len(current_batch)} symbols)[/]")

        # 2. Download
        download_urls = self.downloader.download(current_batch, self.config)
        
        # 3. Download & Extract Execution
        with Progress() as progress:
            dl_task = progress.add_task("[cyan]Downloading...", total=len(download_urls))
            ex_task = progress.add_task("[green]Extracting...", total=len(download_urls))
            
            with ThreadPoolExecutor(max_workers=self.config.max_workers) as dl_executor, \
                 ThreadPoolExecutor(max_workers=self.config.max_extract_workers) as ex_executor:
                
                def process_download(url):
                    parts = url.split('/')
                    if self.config.asset_type == "spot":
                        symbol = parts[7]
                    elif self.config.asset_type == "option":
                        symbol = parts[7]
                    else:
                        symbol = parts[8]
                    final_path = os.path.join(self.config.destination_dir, self.config.asset_type, symbol, self.config.data_frequency)
                    os.makedirs(final_path, exist_ok=True)
                    
                    try:
                        content = self.downloader.download_file(url, final_path, self.config)
                        ex_executor.submit(self.extractor.extract, content, final_path, self.config).add_done_callback(
                            lambda _: progress.advance(ex_task)
                        )
                        progress.advance(dl_task)
                    except Exception:
                        pass # Error handled in downloader

                futures = [dl_executor.submit(process_download, url) for url in download_urls]
                for _ in as_completed(futures):
                    pass

        # 4. Verify
        self.verifier.verify(current_batch, self.config)
        
        # 5. Load
        self.loader.load(current_batch, self.config)
        
        self.console.print("[bold green]\nPipeline execution completed successfully.[/]")
