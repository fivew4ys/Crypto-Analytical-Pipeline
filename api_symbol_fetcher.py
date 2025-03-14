import requests
from typing import List
from rich.console import Console
from natsort import natsorted
from config import AppConfig
from interfaces import IFetcher

class ApiSymbolFetcher(IFetcher):
    """Fetches symbols from Binance API."""
    
    def __init__(self):
        self.console = Console()
        self.endpoints = {
            "spot": "https://api.binance.com/api/v3/exchangeInfo",
            "um": "https://fapi.binance.com/fapi/v1/exchangeInfo",
            "cm": "https://dapi.binance.com/dapi/v1/exchangeInfo"
        }

    def get_symbols(self, config: AppConfig) -> List[str]:
        """Get all symbols for the given asset type using API."""
        self.console.print(f"[bold blue]Fetching symbols for {config.asset_type} via API...[/]")
        
        url = self.endpoints.get(config.asset_type)
        if not url:
            self.console.print(f"[bold red]Invalid asset type: {config.asset_type}[/]")
            return []

        try:
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()
            
            all_symbols = [s['symbol'] for s in data['symbols']]
            
            # Filter symbols based on status if needed (usually we want TRADING)
            # But the reference just grabs all. We'll stick to reference logic but maybe add status check?
            # Reference: return list(map(lambda symbol: symbol['symbol'], json.loads(response)['symbols']))
            # It blindly returns all. I'll do the same to match reference.
            
        except Exception as e:
            self.console.print(f"[bold red]Error fetching symbols from API: {e}[/]")
            return []

        if config.symbol_suffix:
            filtered_symbols = []
            for symbol in all_symbols:
                for suffix in config.symbol_suffix:
                    if symbol.endswith(suffix):
                        filtered_symbols.append(symbol)
                        break
            all_symbols = filtered_symbols

        return natsorted(all_symbols)
