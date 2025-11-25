from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
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
    
    # Trigger dbt transformations after stocks are loaded
    # Note: If dbt fails, check the run_dbt_transformations DAG logs for details
    trigger_dbt = TriggerDagRunOperator(
        task_id="trigger_dbt_transformations",
        trigger_dag_id="run_dbt_transformations",
        wait_for_completion=True,
        poke_interval=30,  # Check every 30 seconds
    )
    
    fetch_task >> trigger_dbt