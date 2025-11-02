## Project setup

## Running the Project:

1.  Clone the repository to your computer

2.  Open the project folder in your terminal

3.  Go inside the folder 'hw2'

    ```bash
    cd hw2
    ```

4.  Run docker

    ```bash
    docker-compose up -d
    ```

5.  Airflow is scheduled to run every day at 22:00 UTC, to run it manually:

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

## Example analytical queries

## MongoDB

In MongoDB, we store the Forbes 2000 companies dataset from Kaggle, along with some additional data from the yFinance API. We used the yFinance API in this step because the Kaggle dataset lacked a reliable primary key. To address this, we used the Ticker as a surrogate key and mapped each company name from Kaggle to its corresponding Ticker in yFinance.

We also performed some transformations on the data. Some companies had to be discarded from MongoDB for the following reasons:

1. **Missing in yFinance API:** Some companies, especially European or Asian ones, were not available in yFinance. For example, we could not find a ticker for **“Bâloise Group”** programmatically.

2. **Special characters in names:** Some company names contained non-Latin characters, which caused encoding issues. For example, **“Bâloise Group”** was transformed to **“BÃ¢loise Group”** by the yFinance API, making it impossible to search for in yFinance.

3. **Invalid market data:** Two companies were discarded because their market indicators (e.g., sales in billions) were in an invalid format. For example, **“Quinenco”** had a profit value recorded as **“0.703.6”**, which we could not interpret.

In total, we had to discard 117 companies because we could not find their data in yFinance, and 2 additional companies due to invalid values in the Kaggle dataset. Altogether, we lost 119 companies from Kaggle dataset.

## Troubleshooting

If for some reason airflow-webserver doesn't come up when running docker, try starting airflow-init again and wait for webserver to come up (takes ~1 minute).
