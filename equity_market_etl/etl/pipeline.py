import yfinance as yf
import pandas as pd
from config.settings import tickers, data_frequency
from indicators.technicals import  add_all_indicators
from db.postgres import write_dataframe
from data_ingestion.fetch_ohlcv import sanitize_table_name, run_etl
import re



def run_transform():
    raw_data, data = run_etl(tickers)
    for ticker in tickers:
        print(f"Processing {ticker}...")
        raw_data, data = run_etl(tickers[0])
        table_name = sanitize_table_name(ticker)
        data = add_all_indicators(data)
        write_dataframe(raw_data, f"raw_{table_name}", if_exists="replace")
        write_dataframe(raw_data, f"stg_{table_name}", if_exists="replace")
        write_dataframe(data, f"core_{table_name}", if_exists="replace")
        print(f"Data with indicators written to {table_name} for {ticker}")

    # data = add_all_indicators(data)
    #         table_name = sanitize_table_name(ticker)
            
    #         write_dataframe(raw_data, f"raw_{table_name}")
    #         write_dataframe(data, f"stg_{table_name}")
    #         print(f"Data with indicators written to {table_name}")
