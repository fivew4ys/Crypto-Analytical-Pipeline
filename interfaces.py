from abc import ABC, abstractmethod
from typing import List, Any
from config import AppConfig

class IFetcher(ABC):
    """Interface for fetching symbols."""
    @abstractmethod
    def get_symbols(self, config: AppConfig) -> List[str]:
        pass

class IDownloader(ABC):
    """Interface for downloading files."""
    @abstractmethod
    def download(self, urls: List[str], config: AppConfig) -> List[str]:
        pass

class IExtractor(ABC):
    """Interface for extracting files."""
    @abstractmethod
    def extract(self, files: List[str], config: AppConfig) -> None:
        pass

class IVerifier(ABC):
    """Interface for verifying data."""
    @abstractmethod
    def verify(self, symbols: List[str], config: AppConfig) -> None:
        pass
