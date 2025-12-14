import zipfile
import os
from io import BytesIO
from typing import List
from rich.console import Console
from .config import AppConfig
from .interfaces import IExtractor

class Extractor(IExtractor):
    """Handles extraction of zip files."""
    
    def __init__(self):
        self.console = Console()

    def extract(self, zip_content: bytes, dest_path: str, config: AppConfig) -> int:
        """Extract CSV files from zip content."""
        extracted_count = 0
        try:
            with zipfile.ZipFile(BytesIO(zip_content)) as zip_file:
                for member in zip_file.namelist():
                    filename = os.path.basename(member)
                    if not filename.endswith(".csv"):
                        continue
                    
                    extracted_path = os.path.join(dest_path, filename)
                    if not os.path.exists(extracted_path):
                        with zip_file.open(member) as source, open(extracted_path, "wb") as target:
                            target.write(source.read())
                        extracted_count += 1
        except Exception as e:
            self.console.print(f"[bold red]Error extracting: {e}[/]")
        return extracted_count
