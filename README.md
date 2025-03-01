# Binance Data Downloader

A robust Python script for downloading and extracting historical market data from Binance Vision. This tool supports parallel downloading, automatic extraction, and data verification for both Spot and Futures markets.

## Features

- **Parallel Processing**: Utilizes multi-threading for fast downloads and extractions.
- **Resilience**: Implements retry logic and error handling for network requests.
- **Verification**: Automatically checks for file completeness and date continuity.
- **Smart Batching**: Handles large datasets by splitting requests into manageable batches.
- **Flexible Configuration**: Supports various asset types, time periods, and data frequencies.

## Requirements

- Python 3.12+
- uv (for dependency management)

## Installation

1. Clone the repository:
   ```bash
   git clone <repository-url>
   cd binance-downloader
   ```

2. Install dependencies using uv:
   ```bash
   uv sync
   ```

## Usage

Run the script using `uv run`:

```bash
uv run main.py
```

### Configuration

Modify the parameters in the `__main__` block of `main.py` to customize the download:

- `asset_type`: "spot", "um" (USD-M Futures), or "cm" (COIN-M Futures)
- `time_period`: "daily" or "monthly"
- `data_type`: "klines", "trades", etc.
- `data_frequency`: "1m", "1h", "1d", etc.
- `destination_dir`: Directory to save downloaded data
- `symbol_suffix`: Filter symbols (e.g., ["USDT"])
- `batch_number` & `total_batches`: For distributed downloading

## Versioning

Current Version: 0.1.0

## License

[License Name]
