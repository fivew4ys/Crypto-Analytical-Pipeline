import requests
from typing import List
from xml.etree import ElementTree
from rich.console import Console
from natsort import natsorted
from config import AppConfig
from interfaces import IFetcher

class SymbolFetcher(IFetcher):
    """Fetches symbols from Binance S3 bucket."""
    
    def __init__(self):
        self.console = Console()
        self.s3_base_url = "https://s3-ap-northeast-1.amazonaws.com/data.binance.vision"

    def get_symbols(self, config: AppConfig) -> List[str]:
        """Get all symbols for the given asset type with optional suffix filtering."""
        self.console.print(f"[bold blue]Fetching symbols for {config.asset_type}...[/]")
        
        if config.asset_type == "spot":
            prefix = f"data/spot/{config.time_period}/{config.data_type}/"
        else:
            prefix = f"data/futures/{config.asset_type}/{config.time_period}/{config.data_type}/"

        delimiter = "/"
        marker = None
        all_symbols = []

        while True:
            params = {"prefix": prefix, "delimiter": delimiter}
            if marker:
                params["marker"] = marker

            try:
                response = requests.get(self.s3_base_url, params=params)
                response.raise_for_status()
            except requests.exceptions.RequestException as e:
                self.console.print(f"[bold red]Error fetching symbol list: {e}[/]")
                return []

            try:
                tree = ElementTree.fromstring(response.content)
            except Exception as e:
                self.console.print(f"[bold red]Error parsing XML: {e}[/]")
                return []

            namespace = {'s3': 'http://s3.amazonaws.com/doc/2006-03-01/'}
            common_prefixes = tree.findall(".//s3:CommonPrefixes/s3:Prefix", namespaces=namespace)
            if not common_prefixes:
                common_prefixes = tree.findall(".//CommonPrefixes/Prefix")

            for common_prefix in common_prefixes:
                symbol_path = common_prefix.text
                if config.asset_type == "spot":
                    symbol = symbol_path.replace(prefix, "").strip("/")
                else:
                    symbol = symbol_path.replace(prefix, "").split('/')[0]
                if symbol and symbol not in all_symbols:
                    all_symbols.append(symbol)

            marker_element = tree.find(".//s3:NextMarker", namespaces=namespace)
            if marker_element is None:
                marker_element = tree.find(".//NextMarker")

            if marker_element is not None and marker_element.text:
                marker = marker_element.text
            else:
                break

        if config.symbol_suffix:
            filtered_symbols = []
            for symbol in all_symbols:
                for suffix in config.symbol_suffix:
                    if symbol.endswith(suffix):
                        filtered_symbols.append(symbol)
                        break
            all_symbols = filtered_symbols

        return natsorted(all_symbols)
