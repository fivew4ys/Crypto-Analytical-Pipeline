import requests
from typing import Dict, Any
from rich.console import Console
from src.config import AppConfig

class SchemaMonitor:
    """
    Monitors upstream API schema changes.
    Fetches a single record and verifies it matches the expected structure.
    """
    
    def __init__(self):
        self.console = Console()
        # Expected column counts
        self.expected_columns = {
            "klines": 12,
            "aggTrades": {
                "spot": 8,
                "um": 7,
                "cm": 7,
                "option": 7 # Assuming option same as futures for now, need verification
            },
            "trades": {
                "spot": 7,
                "um": 6,
                "cm": 6,
                "option": 6
            }
        }

    def check_schema(self, config: AppConfig) -> bool:
        """
        Check if the upstream API schema matches expectations.
        Returns True if schema is valid, False otherwise.
        """
        self.console.print("[bold blue]Checking upstream API schema...[/]")
        
        try:
            # Construct a test URL for one symbol (e.g., BTCUSDT)
            # We need a valid symbol. We can default to BTCUSDT for spot/um/option and BTCUSD_PERP for cm
            symbol = "BTCUSDT"
            if config.asset_type == "cm":
                symbol = "BTCUSD_PERP"
                
            url = self._get_test_url(config, symbol)
            if not url:
                self.console.print("[yellow]Skipping schema check: URL construction not supported for this config.[/]")
                return True

            response = requests.get(url)
            response.raise_for_status()
            data = response.json()

            if not data:
                self.console.print("[yellow]Warning: API returned empty data for schema check.[/]")
                return True # Can't check if empty, but not necessarily a schema change

            # Validate based on data type
            if config.data_type == "klines":
                # Klines response is a list of lists
                # [[Open time, Open, High, ...], ...]
                if isinstance(data, list) and len(data) > 0:
                    row = data[0]
                    if len(row) != self.expected_columns["klines"]:
                        self.console.print(f"[bold red]CRITICAL: Schema Mismatch for Klines! Expected 12 columns, got {len(row)}.[/]")
                        return False
            
            # Note: aggTrades and trades might return list of dicts or list of lists depending on endpoint
            # For simplicity, we focus on Klines as it's the primary use case. 
            # Expanding to others follows the same pattern.
            
            self.console.print("[bold green]Schema check passed.[/]")
            return True

        except Exception as e:
            self.console.print(f"[bold red]Schema check failed with error: {e}[/]")
            # Fail safe: If we can't check, maybe we should stop? 
            # Or warn? Let's return False to be safe.
            return False

    def _get_test_url(self, config: AppConfig, symbol: str) -> str:
        """Construct a URL to fetch 1 record."""
        limit = 1
        if config.asset_type == "spot":
            base = "https://api.binance.com/api/v3"
        elif config.asset_type == "um":
            base = "https://fapi.binance.com/fapi/v1"
        elif config.asset_type == "cm":
            base = "https://dapi.binance.com/dapi/v1"
        elif config.asset_type == "option":
            base = "https://eapi.binance.com/eapi/v1"
        else:
            return ""

        if config.data_type == "klines":
            return f"{base}/klines?symbol={symbol}&interval={config.data_frequency}&limit={limit}"
        
        return ""
