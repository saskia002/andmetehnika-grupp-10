from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.api.common.experimental.trigger_dag import trigger_dag
from pymongo import MongoClient
import pandas as pd
import requests
import json
import logging
import os

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "http://clickhouse")
CLICKHOUSE_PORT = os.getenv("CLICKHOUSE_PORT", "8123")
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "etl")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "pass")

def load_companies_from_mongodb(**context):
    client = MongoClient("mongodb://root:root@mongodb:27017")
    db = client["forbes_2000"]
    collection = db["companies"]

    companies = list(collection.find({}, {"_id":0})) #removing ids since don't need them
    df = pd.DataFrame(companies)
    logging.info(f"loaded {len(df)} companies from MongoDB")
    df_dict = df.where(pd.notnull(df), None).to_dict(orient="records")

    execution_date_utc = context["execution_date"].isoformat()
    context["ti"].xcom_push(key="companies_dict", value=df_dict)
    context["ti"].xcom_push(key="execution_date_utc", value=execution_date_utc)
    
    return df_dict

def insert_companies_to_bronze(**context):
    """Insert company data to Bronze layer in ClickHouse with duplicate prevention"""
    # Get companies data from XCom
    ti = context["ti"]
    companies_dict = ti.xcom_pull(task_ids="load_companies", key="companies_dict")
    
    if not companies_dict:
        logging.warning("No companies data to insert")
        return
    
    # Check for existing records to prevent duplicates
    url = f"{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}/"
    auth = None
    if CLICKHOUSE_USER or CLICKHOUSE_PASSWORD:
        auth = (CLICKHOUSE_USER, CLICKHOUSE_PASSWORD)
    
    # Get existing ranks (using FINAL to get deduplicated view)
    try:
        check_sql = "SELECT rank FROM bronze.companies_raw FINAL"
        check_params = {
            "query": check_sql,
            "database": "bronze",
        }
        check_resp = requests.post(url, params=check_params, auth=auth, timeout=30)
        check_resp.raise_for_status()
        existing_ranks = set()
        if check_resp.text.strip():
            for line in check_resp.text.strip().split('\n'):
                if line.strip():
                    existing_ranks.add(int(line.strip()))
        logging.info(f"Found {len(existing_ranks)} existing company records")
    except Exception as e:
        logging.warning(f"Could not check existing records: {e}. Proceeding with insert...")
        existing_ranks = set()
    
    lines = []
    skipped_count = 0
    for record in companies_dict:
        rank = int(record.get("rank", 0) or 0)
        # Skip if this rank already exists
        if rank in existing_ranks:
            skipped_count += 1
            continue
            
        obj = {
            "rank": rank,
            "company": record.get("company", ""),
            "ticker": record.get("ticker", ""),
            "headquarters": record.get("headquarters", ""),
            "industry": record.get("industry", ""),
            "sales_in_millions": float(record.get("sales_in_millions", 0.0)),
            "profit_in_millions": float(record.get("profit_in_millions", 0.0)),
            "assets_in_millions": float(record.get("assets_in_millions", 0.0)),
            "market_value_in_millions": float(record.get("market_value_in_millions", 0.0)),
            "financial_year": int(record.get("financial_year", 0)),
        }
        lines.append(json.dumps(obj))
        existing_ranks.add(rank)  # Track what we're inserting
    
    if skipped_count > 0:
        logging.info(f"Skipped {skipped_count} duplicate company records")
    
    if not lines:
        logging.warning("All records were duplicates, nothing to insert")
        return
    
    # Insert into ClickHouse Bronze layer
    insert_sql = "INSERT INTO bronze.companies_raw FORMAT JSONEachRow"
    params = {
        "query": insert_sql,
        "database": "bronze",
        "input_format_skip_unknown_fields": 1,
    }
    
    try:
        resp = requests.post(
            url,
            params=params,
            data="\n".join(lines).encode("utf-8"),
            headers={"Content-Type": "text/plain; charset=utf-8"},
            auth=auth,
        )
        resp.raise_for_status()
        logging.info(f"Successfully inserted {len(lines)} rows into bronze.companies_raw")
    except requests.exceptions.HTTPError as e:
        error_msg = f"Error inserting into ClickHouse: {e}"
        if hasattr(e.response, 'text'):
            error_msg += f"\nClickHouse response: {e.response.text}"
        logging.error(error_msg)
        raise
    except Exception as e:
        logging.error(f"Error inserting into ClickHouse: {e}")
        raise

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
        python_callable=load_companies_from_mongodb,
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