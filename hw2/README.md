## Project setup

## Running the Project:

1. Clone the repository to your computer

2. Open the project folder in your terminal

3. Go inside the folder 'hw2'

    ```bash
    cd hw2
    ```

4. Run docker

    ```bash
    docker-compose up -d
    ```

5. Airflow is scheduled to run every day at 22:00 UTC, to run it manually:

    5.1. (Optional) Firstly pulling stock info with yfinance API takes time (~15 minutes), to shorten it you can pull less data

        5.1.1. Uncomment this line in plugins/tasks/fetch_yfinance.py file inside the fetch_stock_info() function

            # df = df.iloc[:250]

        
    5.2. Open Airflow on http://localhost:8080/home

    5.3. Log into Airflow 

        username: airflow

        password: airflow
    
    5.4. Trigger the DAG called load_companies_dag - it will trigger fetch_yfinance_dag after completion

    5.5. Wait for the DAGs to be finished and ...

## Airflow

### Graph

<pre>Daily at 22:00 UTC
┌─────────────────────────────┐
│     load_companies_dag      │
│  (Loads data from MongoDB)  │
└─────────────┬───────────────┘
              │ 
              │ Trigger next DAG
              │ with data from MongoDB
              │ 
              ▼
┌──────────────────────────────┐
│      fetch_yfinance_dag      │
│  (Fetch data from yfinance,  │
│  loads data into ClickHouse) │
└──────────────────────────────┘
</pre>

### Overview

The workflow involves two DAGs

<b>DAG 1: </b>load_companies_dag

    Every day at 22:00 UTC Airflow triggers this DAG and executes the function load_companies_from_mongodb, which does the following:
    . Connects to MongoDB to retrieve the company data.
    . Converts the list of company records into a Python dictionary, which is pushed to XCom, so it can be sent to the second DAG.
    Once the company data is loaded, this task triggers the fetch_yfinance_dag DAG

<b>DAG 2: </b>fetch_yfinance_dag 

    This DAG executes the function fetch_stock_info, which does the following:
    . Takes the data from first DAG and does data cleaning - ignores the rows where stock ticker is null, none or nan and cheks that it is unique. (those values that fail these checks are not added to Clickhouse)
    . Fetches stock information for each company from Yahoo Finance using the yfinance package.
    . Each stock's data (such as open price, close price, market cap, etc.) is extracted and formatted as a dictionary.
    . The stock data is passed to the insert_stocks_to_bronze function, which inserts the data into ClickHouse.

The time 22:00 UTC was specifically chosen, so that all the markets around the world would be closed fo that day.

## Example analytical queries


## Troubleshooting

If for some reason airflow-webserver doesn't come up when running docker, try starting airflow-init again and wait for webserver to come up (takes ~1 minute).
