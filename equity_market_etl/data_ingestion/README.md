# data_ingestion

This folder contains modules for fetching raw OHLCV stock data from external APIs (e.g., yFinance, Alpha Vantage).

- `fetch_ohlcv.py`: Functions to download and preprocess OHLCV data for specified tickers and timeframes.
- `__init__.py`: Makes this directory a Python package.

All data ingestion logic should be modular and support easy extension to new data sources. 