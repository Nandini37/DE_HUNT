import yfinance as yf
import pandas as pd
from config.settings import tickers, data_frequency
from indicators.technicals import add_all_indicators
from db.postgres import write_dataframe
import re

def sanitize_table_name(ticker):
    # Lowercase, replace non-alphanumeric with _
    return f"ohlcv_{re.sub(r'[^a-zA-Z0-9]', '_', ticker.lower())}"

def run_etl():
    for ticker in tickers:
        print(f"Processing {ticker}...")
        data = yf.download(ticker, interval=data_frequency)
        data = data.reset_index()
        if isinstance(data.columns, pd.MultiIndex):
            data.columns = [col[0] for col in data.columns]
        data = add_all_indicators(data)
        table_name = sanitize_table_name(ticker)
        write_dataframe(data, table_name)
        print(f"Data with indicators written to {table_name}")
