
import unittest
import os
from unittest.mock import patch
from crypto_pipeline.verifier import Verifier
from crypto_pipeline.config import AppConfig

class TestVerifier(unittest.TestCase):
    def setUp(self):
        self.verifier = Verifier()
        self.config = AppConfig(
            asset_type="spot",
            time_period="daily",
            data_type="klines",
            data_frequency="1d",
            destination_dir="test_data"
        )
    
    def test_verify_file_valid_klines(self):
        # Create a dummy CSV file
        with open("test_valid.csv", "w") as f:
            # 12 columns for klines
            f.write("1,2,3,4,5,6,7,8,9,10,11,12\n")
            
        with patch.object(self.verifier, '_is_valid_timestamp', return_value=True):
            result = self.verifier._verify_file("test_valid.csv", self.config)
            self.assertTrue(result)
            
        os.remove("test_valid.csv")

    def test_verify_file_invalid_schema(self):
        with open("test_invalid.csv", "w") as f:
            # 11 columns (missing one)
            f.write("1,2,3,4,5,6,7,8,9,10,11\n")
            
        with patch.object(self.verifier, '_quarantine_file') as mock_quarantine:
            result = self.verifier._verify_file("test_invalid.csv", self.config)
            self.assertFalse(result)
            mock_quarantine.assert_called_once()
            
        os.remove("test_invalid.csv")

    def test_verify_file_empty(self):
        with open("test_empty.csv", "w") as f:
            pass # Empty
            
        with patch.object(self.verifier, '_quarantine_file') as mock_quarantine:
            result = self.verifier._verify_file("test_empty.csv", self.config)
            self.assertFalse(result)
            mock_quarantine.assert_called_once()
            
        os.remove("test_empty.csv")
