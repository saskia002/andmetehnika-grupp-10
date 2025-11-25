
{{ config(
    materialized='table',
    schema='silver'
) }}

SELECT
    CAST(toUInt32(Year*10000 + Month*100 + Day) AS UInt32) AS DateKey,
    CAST(TradingDate AS Date) AS TradingDate,
    CAST(Year AS UInt32) AS Year,
    CAST(Month AS UInt32) AS Month,
    CAST(Day AS UInt32) AS Day,
    CAST(Quarter AS UInt32) AS Quarter
FROM 
(
    SELECT DISTINCT
        CAST(trading_day AS Date) AS TradingDate,
        CAST(toYear(trading_day) AS UInt32) AS Year,
        CAST(toMonth(trading_day) AS UInt32) AS Month,
        CAST(toDayOfMonth(trading_day) AS UInt32) AS Day,
        CAST(toQuarter(trading_day) AS UInt32) AS Quarter
    FROM bronze.stocks_raw_view
)

