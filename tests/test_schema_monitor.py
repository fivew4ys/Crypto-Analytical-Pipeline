
import unittest
from unittest.mock import patch, MagicMock
from crypto_pipeline.schema_monitor import SchemaMonitor
from crypto_pipeline.config import AppConfig

class TestSchemaMonitor(unittest.TestCase):
    def setUp(self):
        self.monitor = SchemaMonitor()
        self.config = AppConfig(
            asset_type="spot",
            time_period="daily",
            data_type="klines",
            data_frequency="1d"
        )

    @patch('requests.get')
    def test_check_schema_success(self, mock_get):
        mock_response = MagicMock()
        # Klines expects 12 columns
        mock_response.json.return_value = [[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]]
        mock_get.return_value = mock_response

        result = self.monitor.check_schema(self.config)
        self.assertTrue(result)

    @patch('requests.get')
    def test_check_schema_failure(self, mock_get):
        mock_response = MagicMock()
        # Return 11 columns (invalid)
        mock_response.json.return_value = [[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]]
        mock_get.return_value = mock_response

        result = self.monitor.check_schema(self.config)
        self.assertFalse(result)
