
{{ config(
    materialized='table',
    schema='silver'
) }}

SELECT
    CAST(xxHash64(concat(ticker_symbol, exchange)) AS UInt32) AS CompanyKey, -- TO BE FIXED based on clickhouse bronze
    CAST(trading_day AS Date) AS TradingDate,
    CAST(xxHash64(concat(ticker_symbol, exchange)) AS UInt32) AS TickerKey,
    CAST(open_price AS Float64) AS OpenPrice,
    CAST(close_price AS Float64) AS ClosePrice,
    CAST(high_price AS Float64) AS HighPrice,
    CAST(low_price AS Float64) AS LowPrice,
    CAST(market_cap AS Float64) AS MarketCap,
    CAST(dividend AS Float64) AS Dividend
FROM {{ source('bronze', 'stocks_raw') }}
WHERE ticker_symbol IS NOT NULL