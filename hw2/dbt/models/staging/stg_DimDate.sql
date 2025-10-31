

SELECT
    toUInt32(Year*10000 + Month*100 + Day) AS DateKey,
    TradingDate,
    Year,
    Month,
    Day,
    Quarter
FROM 
(
    SELECT DISTINCT
        trading_day AS TradingDate,
        toYear(trading_day) AS Year,
        toMonth(trading_day) AS Month,
        toDayOfMonth(trading_day) AS Day,
        toQuarter(trading_day) AS Quarter
    FROM {{ source('bronze', 'stocks_raw') }}
);

