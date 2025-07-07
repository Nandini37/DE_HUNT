import pandas as pd

# 20-day and 50-day Simple Moving Average (SMA)
def add_sma(df: pd.DataFrame, window: int, price_col: str = 'Close') -> pd.DataFrame:
    col_name = f'SMA_{window}'
    df[col_name] = df[price_col].rolling(window=window, min_periods=1).mean()
    return df

# 14-day Relative Strength Index (RSI)
def add_rsi(df: pd.DataFrame, window: int = 14, price_col: str = 'Close') -> pd.DataFrame:
    delta = df[price_col].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=window, min_periods=1).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=window, min_periods=1).mean()
    rs = gain / loss
    df[f'RSI_{window}'] = 100 - (100 / (1 + rs))
    return df

# MACD (12, 26)
def add_macd(df: pd.DataFrame, fast: int = 12, slow: int = 26, price_col: str = 'Close') -> pd.DataFrame:
    ema_fast = df[price_col].ewm(span=fast, adjust=False).mean()
    ema_slow = df[price_col].ewm(span=slow, adjust=False).mean()
    macd = ema_fast - ema_slow
    df[f'MACD_{fast}_{slow}'] = macd
    return df

# Merge all indicators
def add_all_indicators(df: pd.DataFrame, price_col: str = 'Close') -> pd.DataFrame:
    df = add_sma(df, 20, price_col)
    df = add_sma(df, 50, price_col)
    df = add_rsi(df, 14, price_col)
    df = add_macd(df, 12, 26, price_col)
    return df
