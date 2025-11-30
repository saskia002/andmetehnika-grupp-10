CREATE DATABASE IF NOT EXISTS bronze;
-- Silver and Gold databases will be created and managed by dbt

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
    financial_year UInt32,
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

-- Create user for dbt (manages Silver and Gold layers)
CREATE USER IF NOT EXISTS dbt_user IDENTIFIED BY 'dbt_pass';
GRANT SELECT, INSERT, CREATE, CREATE DATABASE, ALTER ON *.* TO dbt_user;

-- Create user for openmetadata
CREATE USER IF NOT EXISTS openmetadata_user IDENTIFIED BY 'omd_pass';
GRANT SELECT, SHOW ON system.* to openmetadata_user;
GRANT SHOW DATABASES, SHOW TABLES, SELECT ON gold_full_views.* TO openmetadata_user;
GRANT SHOW DATABASES, SHOW TABLES, SELECT ON gold_limited_views.* TO openmetadata_user;

-- Create views for dbt to query (deduplicated, avoiding dbt-clickhouse auto-transformation issues)
CREATE VIEW IF NOT EXISTS bronze.companies_raw_view AS
SELECT 
    company,
    headquarters,
    ticker,
    industry,
    rank,
    sales_in_millions,
    profit_in_millions,
    assets_in_millions,
    market_value_in_millions,
    financial_year
FROM (
    SELECT 
        company,
        headquarters,
        ticker,
        industry,
        rank,
        sales_in_millions,
        profit_in_millions,
        assets_in_millions,
        market_value_in_millions,
        financial_year,
        ROW_NUMBER() OVER (PARTITION BY company, headquarters, financial_year ORDER BY _ingested_at DESC) AS rn
    FROM bronze.companies_raw FINAL
)
WHERE rn = 1;

CREATE VIEW IF NOT EXISTS bronze.stocks_raw_view AS
SELECT 
    ticker_symbol,
    sector,
    open_price,
    close_price,
    high_price,
    low_price,
    market_cap,
    currency,
    exchange,
    dividend,
    execution_date_utc,
    trading_day,
    month,
    day_of_week,
    quarter,
    season
FROM (
    SELECT 
        ticker_symbol,
        sector,
        open_price,
        close_price,
        high_price,
        low_price,
        market_cap,
        currency,
        exchange,
        dividend,
        execution_date_utc,
        trading_day,
        month,
        day_of_week,
        quarter,
        season,
        ROW_NUMBER() OVER (PARTITION BY ticker_symbol, execution_date_utc ORDER BY _ingested_at DESC) AS rn
    FROM bronze.stocks_raw FINAL
)
WHERE rn = 1;

