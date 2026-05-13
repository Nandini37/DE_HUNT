import os

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", 5432)),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", "password"),
    "database": os.getenv("DB_NAME", "equity_market_dev"),
}

# List of tickers to process
tickers = ["CL=F"]
# Data frequency (e.g., '1d' for daily, '1h' for hourly)
data_frequency = "1d"
