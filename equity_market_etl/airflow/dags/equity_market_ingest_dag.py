from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'nandini',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'equity_market_daily_ingest',
    default_args=default_args,
    description='Daily data ingestion for equity market ETL',
    schedule_interval='0 13 * * *',  # Every day at 1 pm
    start_date=datetime(2025, 7, 21),
    catchup=False,
)

ingest_task = BashOperator(
    task_id='run_data_ingestion',
    bash_command='cd /Users/nandiniwadaskar/Learning/GitHub/DE_HUNT && /Users/nandiniwadaskar/Learning/GitHub/DE_HUNT/equity_market_etl/venv/bin/python equity_market_etl/data_ingestion/fetch_ohlcv.py',
    dag=dag,
)