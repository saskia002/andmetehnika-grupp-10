

SELECT
    abs(mod(cast(hash(ticker_symbol) as bigint), 1000000000)) as TickerKey, -- Generated Primary Key
    ticker_symbol as TickerSymbol,
    exchange as Exchange,
    Class, -- ei näe allikas
    currency as Currency
FROM  {{ source('bronze', 'stocks_raw') }}
WHERE ticker_symbol IS NOT NULL;
