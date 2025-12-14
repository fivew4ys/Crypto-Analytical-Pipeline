# Google Colab Usage Example

# 1. Install the Package
# !pip install git+https://github.com/fivew4ys/Crypto-Analytical-Pipeline.git

# 2. Import Pipeline
from crypto_pipeline.pipeline import Pipeline

# 3. Define Configuration
config = {
    "asset_type": "spot",
    "time_period": "daily",
    "data_type": "klines",
    "data_frequency": "1d",
    "destination_dir": "./binance_data",
    "symbol_suffix": ["USDT"],
    "fetch_method": "xml",  # IMPORTANT: Use XML for Colab to avoid API blocks
    "db_path": "crypto_data.duckdb" # Data will be loaded here
}

# 4. Run Pipeline
pipeline = Pipeline(config)
pipeline.run()

# 5. Query Data with DuckDB
import duckdb
con = duckdb.connect("crypto_data.duckdb")
df = con.execute("SELECT * FROM klines LIMIT 5").df()
print(df)
