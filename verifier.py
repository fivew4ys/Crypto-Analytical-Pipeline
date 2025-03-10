import os
import re
from datetime import datetime
from dateutil.relativedelta import relativedelta
from typing import List
from rich.console import Console
from config import AppConfig
from interfaces import IVerifier

class Verifier(IVerifier):
    """Handles verification of downloaded data."""
    
    def __init__(self):
        self.console = Console()

    def verify(self, symbols: List[str], config: AppConfig) -> None:
        """Check date continuity in downloaded CSVs."""
        self.console.print("\n[bold blue]Verifying date continuity...[/]")
        date_format = "%Y-%m-%d" if config.time_period == "daily" else "%Y-%m"
        pattern = re.compile(r'(\d{4}-\d{2}(?:-\d{2})?)\.csv$')

        for symbol in symbols:
            symbol_dir = os.path.join(config.destination_dir, config.asset_type, symbol, config.data_frequency)
            if not os.path.exists(symbol_dir):
                self.console.print(f"[red]Missing directory for {symbol}[/]")
                continue

            csv_files = [f for f in os.listdir(symbol_dir) if f.endswith(".csv")]
            dates = []
            for f in csv_files:
                match = pattern.search(f)
                if match:
                    try:
                        dates.append(datetime.strptime(match.group(1), date_format))
                    except ValueError:
                        continue

            if not dates:
                self.console.print(f"[yellow]No valid dates for {symbol}[/]")
                continue

            dates.sort()
            min_date, max_date = dates[0], dates[-1]
            expected = []
            current = min_date
            delta = relativedelta(days=1) if config.time_period == "daily" else relativedelta(months=1)
            
            while current <= max_date:
                expected.append(current)
                current += delta

            missing = [d.strftime(date_format) for d in expected if d not in dates]
            if missing:
                self.console.print(f"\n[bold red]{symbol} missing {len(missing)} dates[/]")
                self.console.print(f"First: {min_date.strftime(date_format)}")
                self.console.print(f"Last: {max_date.strftime(date_format)}")
                self.console.print("Last 5 missing:")
                for d in missing[-5:]:
                    self.console.print(f"  {d}")
            else:
                self.console.print(f"[green]{symbol}: Complete ({len(dates)} files)[/]")
