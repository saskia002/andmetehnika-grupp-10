
{{ config(
    materialized='table',
    schema='silver'
) }}

SELECT
    -- Foreign keys for star schema joins
    CAST(toUInt32(toYear(trading_day)*10000 + toMonth(trading_day)*100 + toDayOfMonth(trading_day)) AS UInt32) AS DateKey,
    CAST(xxHash64(concat(ticker_symbol, COALESCE(exchange, ''))) AS UInt32) AS CompanyKey,
    CAST(xxHash64(concat(ticker_symbol, COALESCE(exchange, ''))) AS UInt32) AS TickerKey,
    -- Fact measures
    CAST(trading_day AS Date) AS TradingDate,
    CAST(COALESCE(open_price, 0) AS Float64) AS OpenPrice,
    CAST(COALESCE(close_price, 0) AS Float64) AS ClosePrice,
    CAST(COALESCE(high_price, 0) AS Float64) AS HighPrice,
    CAST(COALESCE(low_price, 0) AS Float64) AS LowPrice,
    CAST(COALESCE(market_cap, 0) AS Float64) AS MarketCap,
    CAST(COALESCE(dividend, 0) AS Float64) AS Dividend
FROM bronze.stocks_raw_view
WHERE ticker_symbol IS NOT NULL