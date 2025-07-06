# config

This folder contains configuration files for the ETL pipeline.

- `settings.py`: Centralized configuration for tickers, frequency, API keys, database connection, and other pipeline parameters.
- `__init__.py`: Makes this directory a Python package.

Sensitive information (API keys, passwords) should be managed via environment variables or a `.env` file (not committed to version control). 