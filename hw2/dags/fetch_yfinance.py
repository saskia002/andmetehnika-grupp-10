from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import yfinance as yf
import pandas as pd

COMPANIES_PATH = "/opt/airflow/companies/companies.csv"

def fetch_stock_info():
    df = pd.read_csv(COMPANIES_PATH)
    data = []
    for _, row in df.iterrows():
        ticker = row.get('ticker')
        if not ticker or pd.isna(ticker):
            print(f"Ticker missing for company: {row.get('company')}")
            continue
        try:
            stock = yf.Ticker(ticker)
            info = stock.info
            if info: 
                stock_data = {
                    'TickerSymbol': ticker, 
                    'sector': info.get('sector'),
                    'OpenPrice': info.get('open'),
                    'ClosePrice': info.get('previousClose'),
                    'HighPrice': info.get('dayHigh'),
                    'LowPrice': info.get('dayLow'),
                    'MarketCap': info.get('marketCap'),
                    }
                print(stock_data)
                data.append({
                    'TickerSymbol': ticker, 
                    'sector': info.get('sector'),
                    'OpenPrice': info.get('open'),
                    'ClosePrice': info.get('previousClose'),
                    'HighPrice': info.get('dayHigh'),
                    'LowPrice': info.get('dayLow'),
                    'MarketCap': info.get('marketCap'),
                    })
            else:
                print(f"No info returned for {ticker}")

        except Exception as e:
            print(f"Error fetching {ticker}: {e}")
    #pd.DataFrame(data).to_csv("")
    print(f"Got data about {len(data)} companie stocks")


with DAG(
    dag_id="fetch_yfinance_dag",
    start_date=datetime(2025, 10, 26),
    schedule=None,  # every minute
    catchup=False,
) as dag:

    fetch_task = PythonOperator(
        task_id="fetch_yfinance",
        python_callable=fetch_stock_info
    )