from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.api.common.experimental.trigger_dag import trigger_dag
from pymongo import MongoClient
import pandas as pd


def load_companies_from_mongodb(**context):
    client = MongoClient("mongodb://root:root@mongodb:27017")
    db = client["forbes_2000"]
    collection = db["companies"]

    companies = list(collection.find({}, {"_id":0})) #removing ids since don't need them
    df = pd.DataFrame(companies)
    print(f"loaded {len(df)} companies from MongoDB")
    df_dict = df.where(pd.notnull(df), None).to_dict(orient="records")

    execution_date_utc = context["execution_date"].isoformat()
    context["ti"].xcom_push(key="companies_dict", value=df_dict)
    context["ti"].xcom_push(key="execution_date_utc", value=execution_date_utc)