from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime
import yfinance as yf
import pandas as pd
import ast
import logging
import requests
import json
import os

COMPANIES_PATH = "/opt/airflow/companies/companies.csv"
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "http://clickhouse")
CLICKHOUSE_PORT = os.getenv("CLICKHOUSE_PORT", "8123")
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "etl")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "pass")

def insert_stocks_to_bronze(data: list, date_data: dict, execution_date_utc: str):
    """Insert stock data to Bronze layer in ClickHouse with duplicate prevention"""
    if not data:
        logging.info("No stock data to insert")
        return
    try:
        dt_execution = pd.Timestamp(execution_date_utc)
        execution_date_formatted = dt_execution.strftime("%Y-%m-%d %H:%M:%S")
    except Exception as e:
        logging.warning(f"Error parsing execution_date_utc: {e}, using as-is")
        execution_date_formatted = execution_date_utc
        trading_day_str = None
    if date_data.get("trading_day"):
        if isinstance(date_data["trading_day"], str):
            trading_day_str = date_data["trading_day"]
        elif isinstance(date_data["trading_day"], pd.Timestamp):
            trading_day_str = date_data["trading_day"].strftime("%Y-%m-%d")
        elif hasattr(date_data["trading_day"], 'isoformat'): 
            trading_day_str = date_data["trading_day"].isoformat()
        else:
            trading_day_str = str(date_data["trading_day"])
    
    # Check for existing records to prevent duplicates
    url = f"{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}/"
    auth = None
    if CLICKHOUSE_USER or CLICKHOUSE_PASSWORD:
        auth = (CLICKHOUSE_USER, CLICKHOUSE_PASSWORD)
    
    # Get existing ticker_symbol + execution_date_utc combinations (using FINAL to get deduplicated view)
    try:
        check_sql = "SELECT ticker_symbol, execution_date_utc FROM bronze.stocks_raw FINAL"
        check_params = {
            "query": check_sql,
            "database": "bronze",
        }
        check_resp = requests.post(url, params=check_params, auth=auth, timeout=30)
        check_resp.raise_for_status()
        existing_records = set()
        if check_resp.text.strip():
            for line in check_resp.text.strip().split('\n'):
                if line.strip() and '\t' in line:
                    parts = line.strip().split('\t')
                    if len(parts) >= 2:
                        existing_records.add((parts[0], parts[1]))
        logging.info(f"Found {len(existing_records)} existing stock records")
    except Exception as e:
        logging.warning(f"Could not check existing records: {e}. Proceeding with insert...")
        existing_records = set()
    
    lines = []
    skipped_count = 0
    for record in data:
        market_cap_value = record.get("market_cap")
        if market_cap_value is None or pd.isna(market_cap_value):
            market_cap_value = None  
        else:
            market_cap_value = int(market_cap_value)
        
        ticker_symbol = str(record.get("ticker_symbol", ""))
        # Skip if this ticker_symbol + execution_date_utc already exists
        if (ticker_symbol, execution_date_formatted) in existing_records:
            skipped_count += 1
            continue
        
        obj = {
            "ticker_symbol": ticker_symbol,
            "sector": record.get("sector") if record.get("sector") is not None else None,
            "open_price": float(record.get("open_price")) if record.get("open_price") is not None and not pd.isna(record.get("open_price")) else None,
            "close_price": float(record.get("close_price")) if record.get("close_price") is not None and not pd.isna(record.get("close_price")) else None,
            "high_price": float(record.get("high_price")) if record.get("high_price") is not None and not pd.isna(record.get("high_price")) else None,
            "low_price": float(record.get("low_price")) if record.get("low_price") is not None and not pd.isna(record.get("low_price")) else None,
            "market_cap": market_cap_value,
            "currency": record.get("currency") if record.get("currency") is not None else None,
            "exchange": record.get("exchange") if record.get("exchange") is not None else None,
            "dividend": float(record.get("dividend")) if record.get("dividend") is not None and not pd.isna(record.get("dividend")) else None,
            "execution_date_utc": execution_date_formatted,
            "trading_day": trading_day_str,
            "month": int(date_data.get("month", 0)),
            "day_of_week": str(date_data.get("day_of_week", "")),
            "quarter": int(date_data.get("quarter", 0)),
            "season": str(date_data.get("season", "")),
        }
        lines.append(json.dumps(obj, ensure_ascii=False, default=str))
        existing_records.add((ticker_symbol, execution_date_formatted))  # Track what we're inserting
    
    if skipped_count > 0:
        logging.info(f"Skipped {skipped_count} duplicate stock records")
    
    if not lines:
        logging.warning("All records were duplicates, nothing to insert")
        return
    
    # Insert into ClickHouse Bronze layer
    insert_sql = "INSERT INTO bronze.stocks_raw FORMAT JSONEachRow"
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
        logging.info(f"Successfully inserted {len(lines)} rows into bronze.stocks_raw")
    except requests.exceptions.HTTPError as e:
        error_msg = f"Error inserting into ClickHouse: {e}"
        if hasattr(e.response, 'text'):
            error_msg += f"\nClickHouse response: {e.response.text}"
        logging.error(error_msg)
        if lines:
            logging.error(f"Sample data (first record): {lines[0]}")
        raise
    except Exception as e:
        logging.error(f"Error inserting into ClickHouse: {e}")
        if lines:
            logging.error(f"Sample data (first record): {lines[0]}")
        raise



def check_if_data_exists(execution_date_utc: str) -> bool:
    """Check if data for the given execution date already exists in ClickHouse"""

    dt = pd.Timestamp(execution_date_utc)
    date = dt.date().isoformat()
    
    query = f"""
    SELECT COUNT(*) 
    FROM bronze.stocks_raw
    WHERE trading_day = '{date}'
    """
    
    url = f"{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}/"
    params = {
        "query": query,
        "database": "bronze",
        "input_format_skip_unknown_fields": 1,
    }
    
    auth = None
    if CLICKHOUSE_USER or CLICKHOUSE_PASSWORD:
        auth = (CLICKHOUSE_USER, CLICKHOUSE_PASSWORD)
    
    try:
        response = requests.get(url, params=params, auth=auth)
        response.raise_for_status()
        count = int(response.text.strip())  # Get the number of rows
        if count > 0:
            logging.info(f"Data for trading date {date} already exists in ClickHouse.")
            return True  # Data exists
        else:
            logging.info(f"No data found for trading date {date}.")
            return False  # No data
    except requests.exceptions.RequestException as e:
        logging.error(f"Error querying ClickHouse: {e}")
        return False  # Return False if there's an error querying ClickHouse






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

    if check_if_data_exists(execution_date_utc):
        logging.info(f"Skipping Yahoo Finance pull for {execution_date_utc} as data already exists.")
        return

    #df = df.iloc[:20]  <--------------------- Uncomment this ----------------------->

    #making sure that there are no duplicate tickers in data
    df = df.drop_duplicates(subset=["ticker"], keep="first")

    data = []
    for _, row in df.iterrows():
        ticker = row.get('ticker')
        #checking that ticker is not empty, none or nan value
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
    insert_stocks_to_bronze(data, date_data, execution_date_utc)


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
        
