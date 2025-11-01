from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from tasks.fetch_yfinance import fetch_stock_info


with DAG(
    dag_id="fetch_yfinance_dag",
    start_date=datetime(2025, 10, 26),
    schedule=None, 
    catchup=False,
) as dag:

    fetch_task = PythonOperator(
        task_id="fetch_yfinance",
        python_callable=fetch_stock_info,
        provide_context=True
    )