from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import yfinance as yf
import pandas as pd
import ast
import logging

COMPANIES_PATH = "/opt/airflow/companies/companies.csv"

def fetch_stock_info(**context):
    conf = context["dag_run"].conf

    #somehow check that no data with this date already exists....
    execution_date_utc = conf.get("execution_date_utc")
    logging.info(f"Execution date value: {execution_date_utc}")

    #Date related data for one runtime
    date_data = get_date_data(execution_date_utc)
    logging.info(f"Date data: {date_data}")

    #list of dictonaries, each company separately
    companies_dict = ast.literal_eval(conf.get("companies_dict"))

    df = pd.DataFrame(companies_dict)
    data = []
    for _, row in df.iterrows():
        ticker = row.get('ticker')
        if not ticker or pd.isna(ticker):
            logging.info(f"Ticker missing for company: {row.get('company')}")
            continue
        try:
            stock = yf.Ticker(ticker)
            info = stock.info
            if info:
                data.append({
                    'ticker_symbol': ticker, 
                    'sector': info.get('sector'),
                    'open_price': info.get('open'),
                    'close_price': info.get('previousClose'),
                    'high_price': info.get('dayHigh'),
                    'low_price': info.get('dayLow'),
                    'market_cap': info.get('marketCap'),
                    'currency': info.get('currency'),
                    'exchange': info.get('exchange'),
                    'dividend': info.get('dividendRate'),
                    })
            else:
                logging.info(f"No info returned for {ticker}")
        except Exception as e:
            logging.error(f"Error fetching {ticker}: {e}")
    logging.info(f"Got data about {len(data)} company stocks")


def get_date_data(execution_date_utc):
    dt = pd.Timestamp(execution_date_utc)
    date_data = {
        "execution_date_utc": execution_date_utc,
        "trading_day": dt.date(),
        "month": dt.month,
        "day_of_week": dt.day_name(),
        "quarter": dt.quarter
        }
    if dt.month in [12, 1, 2]:
        date_data["season"] = "Winter"
    elif dt.month in [3, 4, 5]:
        date_data["season"] = "Spring"
    elif dt.month in [6, 7, 8]:
        date_data["season"] = "Summer"
    else:
        date_data["season"] = "Autumn"
    return date_data
        


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