



# --- Data Dump ---

import yfinance as yf
import pandas as pd
from config.settings import tickers, data_frequency
from indicators.technicals import add_all_indicators
from db.postgres import write_dataframe
import re

def sanitize_table_name(ticker):
    # Lowercase, replace non-alphanumeric with _
    return f"ohlcv_{re.sub(r'[^a-zA-Z0-9]', '_', ticker.lower())}"

def run_etl(ticker):
    print(f"Processing {ticker}...")
    data = yf.download(ticker, interval=data_frequency)
    data = data.reset_index()
    if isinstance(data.columns, pd.MultiIndex):
        data.columns = [col[0] for col in data.columns]
    raw_data = data.copy()
    return raw_data, data






