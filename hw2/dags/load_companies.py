from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from pymongo import MongoClient
import pandas as pd

COMPANIES_PATH = "/opt/airflow/companies/companies.csv"


def load_companies_from_mongodb():
    client = MongoClient("mongodb://root:root@mongodb:27017")
    db = client["forbes_2000"]
    collection = db["companies"]

    companies = list(collection.find())
    df = pd.DataFrame(companies)
    df.to_csv(COMPANIES_PATH, index=False)
    print(f"loaded {len(df)} companies from MongoDB")

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
        python_callable=load_companies_from_mongodb
    )
    
    trigger_next = TriggerDagRunOperator(
        task_id="trigger_fetch_yfinance",
        trigger_dag_id="fetch_yfinance_dag",
    )
