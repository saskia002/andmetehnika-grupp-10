from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.api.common.experimental.trigger_dag import trigger_dag
from tasks.load_companies import load_companies_from_iceberg
from tasks.load_companies import insert_companies_to_bronze

#schedule="0 22 * * *",  # all markets are closed

with DAG(
    dag_id="load_companies_dag",
    start_date=datetime(2025, 10, 26),
    schedule="0 22 * * *",  # all markets are closed
    catchup=False,
    max_active_runs=1
) as dag:

    load_task = PythonOperator(
        task_id="load_companies",
        python_callable=load_companies_from_iceberg,
        provide_context=True
    )

    insert_to_bronze_task = PythonOperator(
        task_id="insert_companies_to_bronze",
        python_callable=insert_companies_to_bronze,
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

load_task >> insert_to_bronze_task >> trigger_next