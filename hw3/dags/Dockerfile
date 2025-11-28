FROM apache/airflow:2.8.1


USER root

RUN apt-get update && apt-get install -y build-essential python3-dev

USER airflow

RUN pip install --no-cache-dir apache-airflow==2.8.1 duckdb pyarrow pyiceberg pandas yfinance==0.2.66 multitasking==0.0.11
