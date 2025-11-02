
{{ config(
    materialized='incremental',
    schema='gold',
    order_by=['DateKey', 'CompanyKey', 'TickerKey'],
    partition_by='toYYYYMM(TradingDate)',
    incremental_strategy='delete+insert',
    unique_key=['DateKey', 'CompanyKey', 'TickerKey']
) }}

SELECT
    DateKey,
    CompanyKey,
    TickerKey,
    TradingDate,
    OpenPrice,
    ClosePrice,
    HighPrice,
    LowPrice,
    MarketCap,
    Dividend
FROM {{ ref('stg_FactStock') }}
{% if is_incremental() %}
    WHERE TradingDate > (SELECT max(TradingDate) FROM {{ this }})
{% endif %}