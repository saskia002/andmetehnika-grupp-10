
{{ config(
    materialized='table',
    schema='silver'
) }}

SELECT
    -- Foreign keys for star schema joins
    CAST(toUInt32(toYear(st.trading_day)*10000 + toMonth(st.trading_day)*100 + toDayOfMonth(st.trading_day)) AS UInt32) AS DateKey,
    CAST(xxHash64(concat(co.company,co.headquarters)) AS UInt32) AS CompanyKey, 
    CAST(xxHash64(concat(st.ticker_symbol, COALESCE(st.exchange, ''))) AS UInt32) AS TickerKey,
    -- Fact measures
    --CAST(st.trading_day AS Date) AS TradingDate, --not part of star schema
    CAST(COALESCE(st.open_price, 0) AS Float64) AS OpenPrice,
    CAST(COALESCE(st.close_price, 0) AS Float64) AS ClosePrice,
    CAST(COALESCE(st.high_price, 0) AS Float64) AS HighPrice,
    CAST(COALESCE(st.low_price, 0) AS Float64) AS LowPrice,
    CAST(COALESCE(st.market_cap, 0) AS Float64) AS MarketCap,
    CAST(COALESCE(st.dividend, 0) AS Float64) AS Dividend
FROM bronze.stocks_raw_view st
JOIN bronze.companies_raw co ON co.ticker = st.ticker_symbol
WHERE ticker_symbol IS NOT NULL