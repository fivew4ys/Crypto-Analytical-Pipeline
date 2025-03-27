# Binance Data Downloader

A robust Python script for downloading and extracting historical market data from Binance Vision. This tool supports parallel downloading, automatic extraction, and data verification for both Spot and Futures markets.

## Features

- **Parallel Processing**: Utilizes multi-threading for fast downloads and extractions.
- **Resilience**: Implements retry logic and error handling for network requests.
- **Verification**: Automatically checks for file completeness and date continuity.
- **Smart Batching**: Handles large datasets by splitting requests into manageable batches.
- **Flexible Configuration**: Supports various asset types, time periods, and data frequencies.

## Requirements
uv run main.py
```

### Configuration

Modify the parameters in the `__main__` block of `main.py` or use CLI arguments:

- `asset_type`: "spot", "um" (USD-M Futures), or "cm" (COIN-M Futures)
- `time_period`: "daily" or "monthly"
- `data_type`: "klines", "trades", etc.
- `data_frequency`: "1m", "1h", "1d", etc.
- `destination_dir`: Directory to save downloaded data
- `symbol_suffix`: Filter symbols (e.g., ["USDT"])
- `batch_number` & `total_batches`: For distributed downloading
- `fetch_method`: "api" (default), "xml", or "json"
- `symbol_file`: Path to JSON file (required if fetch_method is "json")

### Symbol Fetching Methods

1.  **API (`--fetch-method api`)**: The default method. Fetches symbols directly from Binance API. Fast and reliable.
2.  **XML (`--fetch-method xml`)**: Scrapes the S3 bucket XML. **Use this in Google Colab** or other environments where the Binance API might be blocked.
3.  **JSON (`--fetch-method json`)**: Loads symbols from a local JSON file. Use `--symbol-file` to specify the path.

### Example: Google Colab (XML Method)

```bash
uv run main.py --fetch-method xml --asset-type spot --data-frequency 1h
```

## Versioning

Current Version: 0.1.0

## License

[License Name]
