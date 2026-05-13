import yfinance as yf
from db.postgres import write_dataframe
import pandas as pd


def main():
# Get symbol OHLC data
    data = yf.download("CL=F")

    # Reset index to move Date from index to column
    data = data.reset_index()

    # Flatten columns if MultiIndex
    if isinstance(data.columns, pd.MultiIndex):
        data.columns = [col[0] for col in data.columns]

    # Dump to PostgreSQL
    table_name = "ohlcv_crude_oil_futures"
    write_dataframe(data, table_name)
    print(f"Data written to {table_name}")





