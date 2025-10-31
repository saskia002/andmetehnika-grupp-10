{% snapshot DimTicker_snapshot %}
{{ config(
    unique_key='TickerKey',
    strategy='check',
    check_cols=['TickerSymbol','Exchange','Class','Currency'] --unfortunately as we had to generate primary key then we cannot demonstrate tickersymbol change, but it would still be accurate to record it's change if we would have  high quality source
) }}

SELECT
    TickerSymbol,
    Exchange,
    Currency,
    Sector,
    _load_datetime
FROM {{ ref('stg_DimTicker') }}
{% endsnapshot %}