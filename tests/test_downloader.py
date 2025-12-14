
import unittest
from unittest.mock import patch, MagicMock
from crypto_pipeline.downloader import Downloader
from crypto_pipeline.config import AppConfig

class TestDownloader(unittest.TestCase):
    def setUp(self):
        self.downloader = Downloader()
        self.config = AppConfig(
            asset_type="spot",
            time_period="daily",
            data_type="klines",
            data_frequency="1d"
        )

    @patch('requests.get')
    def test_download_file_success(self, mock_get):
        mock_response = MagicMock()
        mock_response.content = b"zip_content"
        mock_get.return_value = mock_response

        content = self.downloader.download_file("http://example.com/file.zip", "dest_path", self.config)
        self.assertEqual(content, b"zip_content")

    @patch('requests.get')
    def test_download_file_retry(self, mock_get):
        # Fail twice, succeed on third
        mock_fail = MagicMock()
        mock_fail.raise_for_status.side_effect = Exception("Network Error")
        
        mock_success = MagicMock()
        mock_success.content = b"success"
        
        # side_effect can be an iterable
        import requests
        mock_get.side_effect = [requests.exceptions.RequestException, requests.exceptions.RequestException, mock_success]
        
        content = self.downloader.download_file("http://example.com/file.zip", "dest_path", self.config)
        self.assertEqual(content, b"success")
        self.assertEqual(mock_get.call_count, 3)
