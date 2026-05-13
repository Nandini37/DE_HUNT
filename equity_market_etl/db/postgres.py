import pandas as pd
from sqlalchemy import create_engine
from config.settings import DB_CONFIG

def get_engine():
    url = (
        f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@"
        f"{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    )
    return create_engine(url)

def write_dataframe(df: pd.DataFrame, table_name: str, if_exists: str = 'append'):
    engine = get_engine()
    df.to_sql(table_name, engine, if_exists=if_exists, index=False)
 