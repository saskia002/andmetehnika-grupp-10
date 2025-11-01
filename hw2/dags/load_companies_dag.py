from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.api.common.experimental.trigger_dag import trigger_dag
import sys

sys.path.append('/opt/airflow/tasks')
from tasks.load_companies import load_companies_from_mongodb


with DAG(
    dag_id="load_companies_dag",
    start_date=datetime(2025, 10, 26),
    schedule="0 22 * * *",  # all markets are closed
    catchup=False,
    max_active_runs=1
) as dag:

    load_task = PythonOperator(
        task_id="load_companies",
        python_callable=load_companies_from_mongodb,
        provide_context=True
    )
    
    trigger_next = TriggerDagRunOperator(
        task_id="trigger_fetch_yfinance",
        trigger_dag_id="fetch_yfinance_dag",
        conf={
            "companies_dict": "{{ ti.xcom_pull(task_ids='load_companies', key='companies_dict') }}",
            "execution_date_utc": "{{ ti.xcom_pull(task_ids='load_companies', key='execution_date_utc') }}",
        }
    )

load_task >> trigger_next