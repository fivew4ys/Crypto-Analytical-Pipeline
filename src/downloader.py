import requests
import os
from typing import List
from concurrent.futures import ThreadPoolExecutor, as_completed
from rich.progress import Progress, TaskID
from rich.console import Console
from xml.etree import ElementTree
from config import AppConfig
from interfaces import IDownloader

class Downloader(IDownloader):
    """Handles downloading of files."""
    
    def __init__(self):
        self.console = Console()
        self.s3_base_url = "https://s3-ap-northeast-1.amazonaws.com/data.binance.vision"
        self.download_base_url = "https://data.binance.vision"

    def _fetch_urls_for_prefix(self, prefix: str, config: AppConfig) -> List[str]:
        """Fetch download URLs for a single prefix with retries."""
        download_urls = []
        marker = None
        while True:
            params = {"prefix": prefix, "max-keys": 1000}
            if marker:
                params["marker"] = marker

            for attempt in range(config.retries + 1):
                try:
                    response = requests.get(self.s3_base_url, params=params)
                    response.raise_for_status()
                    break
                except requests.exceptions.RequestException as e:
                    if attempt < config.retries:
                        continue
                    else:
                        self.console.print(f"[bold red]Error fetching URLs for {prefix}: {e}[/]")
                        return download_urls

            try:
                tree = ElementTree.fromstring(response.content)
            except Exception as e:
                self.console.print(f"[bold red]Error parsing XML for {prefix}: {e}[/]")
                return download_urls

            namespace = {'s3': 'http://s3.amazonaws.com/doc/2006-03-01/'}
            contents = tree.findall(".//s3:Contents", namespaces=namespace)
            if not contents:
                contents = tree.findall(".//Contents")

            for content in contents:
                key_element = content.find("./s3:Key", namespaces=namespace)
                if key_element is None:
                    key_element = content.find("./Key")
                if key_element is not None and key_element.text.endswith(".zip"):
                    download_urls.append(f"{self.download_base_url}/{key_element.text}")

            marker_element = tree.find(".//s3:NextMarker", namespaces=namespace)
            if marker_element is None:
                marker_element = tree.find(".//NextMarker")
            
            if marker_element is not None and marker_element.text:
                marker = marker_element.text
            else:
                break

        return download_urls

    def download(self, symbols: List[str], config: AppConfig) -> List[str]:
        """Fetch download URLs in batches."""
        self.console.print(f"[blue]Fetching URLs for {len(symbols)} symbols...[/]")
        download_urls = []
        
        if config.asset_type == "spot":
            base_prefix = f"data/spot/{config.time_period}/{config.data_type}/"
        elif config.asset_type == "option":
            base_prefix = f"data/option/{config.time_period}/{config.data_type}/"
        else:
            base_prefix = f"data/futures/{config.asset_type}/{config.time_period}/{config.data_type}/"

        with Progress() as progress:
            task = progress.add_task("[cyan]Fetching URLs...", total=len(symbols))
            with ThreadPoolExecutor(max_workers=config.max_workers) as executor:
                futures = [executor.submit(self._fetch_urls_for_prefix, f"{base_prefix}{symbol}/{config.data_frequency}/", config) 
                          for symbol in symbols]

                for future in as_completed(futures):
                    download_urls.extend(future.result())
                    progress.advance(task)

        return download_urls

    def download_file(self, url: str, dest_path: str, config: AppConfig) -> bytes:
        """Download a single file and return content."""
        for attempt in range(config.retries + 1):
            try:
                response = requests.get(url)
                response.raise_for_status()
                return response.content
            except requests.exceptions.RequestException as e:
                if attempt == config.retries:
                    self.console.print(f"[bold red]Failed to download {url}: {e}[/]")
                    raise
