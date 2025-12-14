
import unittest
from unittest.mock import patch, MagicMock
from crypto_pipeline.symbol_fetcher import SymbolFetcher
from crypto_pipeline.config import AppConfig

class TestSymbolFetcher(unittest.TestCase):
    def setUp(self):
        self.fetcher = SymbolFetcher()
        self.config = AppConfig(
            asset_type="spot",
            time_period="daily",
            data_type="klines",
            data_frequency="1d"
        )

    @patch('requests.get')
    def test_get_symbols_api_success(self, mock_get):
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "symbols": [
                {"symbol": "BTCUSDT"},
                {"symbol": "ETHUSDT"},
                {"symbol": "BNBBTC"}
            ]
        }
        mock_get.return_value = mock_response
        
        # Test basic fetch
        symbols = self.fetcher.get_symbols(self.config)
        self.assertEqual(symbols, ["BNBBTC", "BTCUSDT", "ETHUSDT"]) # Should be sorted

        # Test filtering
        self.config.symbol_suffix = ["USDT"]
        symbols = self.fetcher.get_symbols(self.config)
        self.assertEqual(symbols, ["BTCUSDT", "ETHUSDT"])

    @patch('requests.get')
    def test_get_symbols_api_failure(self, mock_get):
        mock_get.side_effect = Exception("API Error")
        symbols = self.fetcher.get_symbols(self.config)
        self.assertEqual(symbols, [])
