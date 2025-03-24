import requests
import json
import os
from typing import List
from xml.etree import ElementTree
from rich.console import Console
from natsort import natsorted
from config import AppConfig
from interfaces import IFetcher

class SymbolFetcher(IFetcher):
    """Fetches symbols using various strategies: API, XML (S3), or JSON file."""
    
    def __init__(self):
        self.console = Console()
        self.s3_base_url = "https://s3-ap-northeast-1.amazonaws.com/data.binance.vision"
        self.api_endpoints = {
            "spot": "https://api.binance.com/api/v3/exchangeInfo",
            "um": "https://fapi.binance.com/fapi/v1/exchangeInfo",
            "cm": "https://dapi.binance.com/dapi/v1/exchangeInfo",
            "option": "https://eapi.binance.com/eapi/v1/exchangeInfo"
        }

    def get_symbols(self, config: AppConfig) -> List[str]:
        """Get all symbols based on configuration method."""
        if config.fetch_method == "api":
            return self._get_symbols_api(config)
        elif config.fetch_method == "xml":
            return self._get_symbols_xml(config)
        elif config.fetch_method == "json":
            return self._get_symbols_json(config)
        else:
            self.console.print(f"[bold red]Unknown fetch method: {config.fetch_method}[/]")
            return []

    def _get_symbols_api(self, config: AppConfig) -> List[str]:
        """Fetch symbols from Binance API."""
        self.console.print(f"[bold blue]Fetching symbols for {config.asset_type} via API...[/]")
        
        url = self.api_endpoints.get(config.asset_type)
        if not url:
            self.console.print(f"[bold red]Invalid asset type: {config.asset_type}[/]")
            return []

        try:
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()
            all_symbols = [s['symbol'] for s in data['symbols']]
        except Exception as e:
            self.console.print(f"[bold red]Error fetching symbols from API: {e}[/]")
            return []

        return self._filter_symbols(all_symbols, config)

    def _get_symbols_xml(self, config: AppConfig) -> List[str]:
        """Fetch symbols from S3 XML (useful when API is blocked)."""
        self.console.print(f"[bold blue]Fetching symbols for {config.asset_type} via XML (S3)...[/]")
        
        if config.asset_type == "spot":
            prefix = f"data/spot/{config.time_period}/{config.data_type}/"
        elif config.asset_type == "option":
            prefix = f"data/option/{config.time_period}/{config.data_type}/"
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
                elif config.asset_type == "option":
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

        return self._filter_symbols(all_symbols, config)

    def _get_symbols_json(self, config: AppConfig) -> List[str]:
        """Fetch symbols from a local JSON file."""
        self.console.print(f"[bold blue]Fetching symbols from file: {config.symbol_file}...[/]")
        
        if not config.symbol_file or not os.path.exists(config.symbol_file):
            self.console.print(f"[bold red]Symbol file not found: {config.symbol_file}[/]")
            return []

        try:
            with open(config.symbol_file, 'r') as f:
                data = json.load(f)
                # Expecting a list of strings or a dict with 'symbols' key
                if isinstance(data, list):
                    all_symbols = data
                elif isinstance(data, dict) and 'symbols' in data:
                    all_symbols = data['symbols']
                else:
                    self.console.print("[bold red]Invalid JSON format. Expected list of symbols or dict with 'symbols' key.[/]")
                    return []
        except Exception as e:
            self.console.print(f"[bold red]Error reading symbol file: {e}[/]")
            return []

        return self._filter_symbols(all_symbols, config)

    def _filter_symbols(self, symbols: List[str], config: AppConfig) -> List[str]:
        """Filter symbols based on suffix."""
        if config.symbol_suffix:
            filtered_symbols = []
            for symbol in symbols:
                for suffix in config.symbol_suffix:
                    if symbol.endswith(suffix):
                        filtered_symbols.append(symbol)
                        break
            return natsorted(filtered_symbols)
        return natsorted(symbols)
