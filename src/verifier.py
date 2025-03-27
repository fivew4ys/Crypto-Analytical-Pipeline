import os
import glob
import csv
from typing import List
from rich.console import Console
from src.config import AppConfig
from src.interfaces import IVerifier

class Verifier(IVerifier):
    """Verifies downloaded data integrity."""

    def __init__(self):
        self.console = Console()

    def verify(self, symbols: List[str], config: AppConfig) -> None:
        """
        Verify downloaded data.
        Checks for:
        1. File existence.
        2. Column counts (Schema validation).
        3. Timestamp format (ms vs us for Spot >= 2025).
        """
        self.console.print("[bold blue]Verifying data...[/]")
        
        error_count = 0
        total_files = 0

        for symbol in symbols:
            # Construct path
            if config.asset_type == "spot" or config.asset_type == "option":
                base_path = os.path.join(config.destination_dir, config.asset_type, symbol, config.data_frequency)
            else:
                base_path = os.path.join(config.destination_dir, config.asset_type, symbol, config.data_frequency)
            
            # Get all CSV files
            csv_files = glob.glob(os.path.join(base_path, "*.csv"))
            total_files += len(csv_files)

            for file_path in csv_files:
                if not self._verify_file(file_path, config):
                    error_count += 1
                    self.console.print(f"[red]Verification failed for {os.path.basename(file_path)}[/]")

        if error_count == 0:
            self.console.print(f"[bold green]Verification successful! Checked {total_files} files.[/]")
        else:
            self.console.print(f"[bold red]Verification completed with {error_count} errors.[/]")

    def _verify_file(self, file_path: str, config: AppConfig) -> bool:
        """Verify a single file."""
        try:
            with open(file_path, 'r') as f:
                reader = csv.reader(f)
                header = next(reader, None) # Check first row
                
                if not header:
                    self._quarantine_file(file_path, config, "Empty file")
                    return False # Empty file

                # 1. Schema Validation (Column Count)
                col_count = len(header)
                expected_cols = self._get_expected_columns(config)
                
                if col_count != expected_cols:
                    self._quarantine_file(file_path, config, f"Schema mismatch: Expected {expected_cols} cols, got {col_count}")
                    return False

                # 2. Timestamp Validation (First row check)
                # Open time is usually the first column (index 0)
                open_time = header[0]
                if not self._is_valid_timestamp(open_time, file_path, config):
                     self._quarantine_file(file_path, config, f"Invalid timestamp format: {open_time}")
                     return False

                return True

        except Exception as e:
            self.console.print(f"  [red]Error reading file: {e}[/]")
            return False

    def _quarantine_file(self, file_path: str, config: AppConfig, reason: str):
        """Move invalid file to quarantine directory."""
        import shutil
        
        quarantine_dir = os.path.join(config.destination_dir, "quarantine")
        os.makedirs(quarantine_dir, exist_ok=True)
        
        file_name = os.path.basename(file_path)
        dest_path = os.path.join(quarantine_dir, file_name)
        
        try:
            shutil.move(file_path, dest_path)
            self.console.print(f"  [yellow]Quarantined {file_name}: {reason}[/]")
        except Exception as e:
            self.console.print(f"  [bold red]Failed to quarantine {file_name}: {e}[/]")

    def _get_expected_columns(self, config: AppConfig) -> int:
        """Return expected column count based on data type and asset type."""
        if config.data_type == "klines":
            return 12
        elif config.data_type == "aggTrades":
            if config.asset_type == "spot":
                return 8
            else: # futures (um, cm)
                return 7
        elif config.data_type == "trades":
            if config.asset_type == "spot":
                return 7
            else: # futures
                return 6
        # Default fallback or throw error
        return 0 

    def _is_valid_timestamp(self, timestamp_str: str, file_path: str, config: AppConfig) -> bool:
        """
        Check if timestamp is valid.
        Spot data >= 2025-01-01 uses Microseconds (16 digits).
        Others use Milliseconds (13 digits).
        """
        if not timestamp_str.isdigit():
            return False
            
        timestamp = int(timestamp_str)
        digits = len(timestamp_str)

        # Extract date from filename to check if it's >= 2025
        # Filename format: SYMBOL-FREQ-YEAR-MONTH.csv or SYMBOL-FREQ-DATE.csv
        # Quick heuristic: Check if filename contains "2025" or later
        is_post_2025 = "2025" in file_path or "2026" in file_path # Simple check
        
        if config.asset_type == "spot" and is_post_2025:
            # Expect Microseconds (16 digits)
            # 2025-01-01 00:00:00 UTC = 1735689600000000 us
            return digits == 16
        else:
            # Expect Milliseconds (13 digits)
            # 2020-01-01 = 1577836800000 ms
            return digits == 13
