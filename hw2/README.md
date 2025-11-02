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

            ```bash
            df = df.iloc[:250]
            ```
        
    5.2. Open Airflow on http://localhost:8080/home

    5.3. Log into Airflow 

        username: airflow

        password: airflow
    
    5.4. Trigger the DAG called load_companies_dag - it will trigger fetch_yfinance_dag after completion

    5.5. Wait for the DAGs to be finished and ...

## Airflow

Daily at 22:00 UTC
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


## Example analytical queries


## Troubleshooting

If for some reason airflow-webserver doesn't come up when running docker, try starting airflow-init again and wait for webserver to come up (takes ~1 minute).


