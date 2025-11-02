
{{ config(
    materialized='table',
    schema='silver'
) }}

SELECT
    CAST(xxHash64(concat(ticker_symbol, COALESCE(exchange, ''))) AS UInt32) AS TickerKey,  -- Generated Primary Key
    CAST(ticker_symbol AS String) AS TickerSymbol,
    CAST(COALESCE(exchange, '') AS String) AS Exchange,
    CAST('Equity' AS String) AS Class,--  CAST(Class AS String) AS Class,       -- Keep if exists in source, otherwise handle later
    CAST(COALESCE(currency, '') AS String) AS Currency,
    CAST(COALESCE(sector, '') AS String) AS Sector,
    CAST(_ingested_at AS DateTime) AS _load_datetime
FROM {{ source('bronze', 'stocks_raw') }}
WHERE ticker_symbol IS NOT NULL
