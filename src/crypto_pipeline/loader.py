import os
import duckdb
import glob
from typing import List
from rich.console import Console
from .config import AppConfig
from .interfaces import ILoader

class DuckDBLoader(ILoader):
    """Loads data into DuckDB."""

    def __init__(self):
        self.console = Console()

    def load(self, symbols: List[str], config: AppConfig) -> None:
        """Load downloaded CSVs into DuckDB."""
        if not config.db_path:
            self.console.print("[yellow]No database path provided. Skipping loading.[/]")
            return

        self.console.print(f"[bold blue]Loading data into DuckDB: {config.db_path}...[/]")
        
        try:
            con = duckdb.connect(config.db_path)
            
            # Create table if not exists (assuming klines structure for now)
            # We'll use a generic approach or specific based on data_type
            if config.data_type == "klines":
                self._load_klines(con, symbols, config)
            else:
                self.console.print(f"[yellow]Loading for {config.data_type} not fully implemented yet. Skipping.[/]")
            
            con.close()
            self.console.print("[bold green]Data loading completed.[/]")
            
        except Exception as e:
            self.console.print(f"[bold red]Error loading data into DuckDB: {e}[/]")

    def _load_klines(self, con, symbols: List[str], config: AppConfig):
        """Load klines data."""
        # Create schema
        con.execute("""
            CREATE TABLE IF NOT EXISTS klines (
                open_time BIGINT,
                open DOUBLE,
                high DOUBLE,
                low DOUBLE,
                close DOUBLE,
                volume DOUBLE,
                close_time BIGINT,
                quote_asset_volume DOUBLE,
                number_of_trades BIGINT,
                taker_buy_base_asset_volume DOUBLE,
                taker_buy_quote_asset_volume DOUBLE,
                ignore DOUBLE,
                symbol VARCHAR,
                interval VARCHAR
            )
        """)

        for symbol in symbols:
            # Find all CSVs for this symbol
            if config.asset_type == "spot" or config.asset_type == "option":
                base_path = os.path.join(config.destination_dir, config.asset_type, symbol, config.data_frequency)
            else:
                base_path = os.path.join(config.destination_dir, config.asset_type, symbol, config.data_frequency)
            
            csv_files = glob.glob(os.path.join(base_path, "*.csv"))
            
            if not csv_files:
                continue
                
            self.console.print(f"Loading {len(csv_files)} files for {symbol}...")
            
            # Bulk insert using DuckDB's read_csv_auto
            # We add symbol and interval columns literally
            for csv_file in csv_files:
                # Normalize path for SQL
                csv_file_sql = csv_file.replace("\\", "/")
                
                query = f"""
                    INSERT INTO klines 
                    SELECT *, '{symbol}' as symbol, '{config.data_frequency}' as interval 
                    FROM read_csv_auto('{csv_file_sql}', header=False)
                """
                try:
                    con.execute(query)
                except Exception as e:
                    self.console.print(f"[red]Failed to load {csv_file}: {e}[/]")
