CREATE DATABASE IF NOT EXISTS bronze;
CREATE DATABASE IF NOT EXISTS silver;
CREATE DATABASE IF NOT EXISTS gold;

CREATE TABLE IF NOT EXISTS bronze.companies_raw
(
    rank UInt32,
    company String,
    ticker String,
    headquarters String,
    industry String,
    sales_in_millions Float64,
    profit_in_millions Float64,
    assets_in_millions Float64,
    market_value_in_millions Float64,
    _ingested_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(_ingested_at)
ORDER BY rank;

CREATE TABLE IF NOT EXISTS bronze.stocks_raw
(
    ticker_symbol String,
    sector Nullable(String),
    open_price Nullable(Float64),
    close_price Nullable(Float64),
    high_price Nullable(Float64),
    low_price Nullable(Float64),
    market_cap Nullable(UInt64),
    currency Nullable(String),
    exchange Nullable(String),
    dividend Nullable(Float64),
    execution_date_utc DateTime,
    trading_day Date,
    month UInt8,
    day_of_week String,
    quarter UInt8,
    season String,
    _ingested_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(_ingested_at)
ORDER BY (ticker_symbol, execution_date_utc)
PARTITION BY toYYYYMM(execution_date_utc);

-- Create user for ETL operations
CREATE USER IF NOT EXISTS etl IDENTIFIED BY 'pass';
GRANT SELECT, INSERT, CREATE, CREATE DATABASE ON *.* TO etl;

-- Create user for dbt (for future use)
CREATE USER IF NOT EXISTS dbt_user IDENTIFIED BY 'dbt_pass';
GRANT SELECT, INSERT, CREATE, CREATE DATABASE, ALTER ON *.* TO dbt_user;

