{% snapshot DimTicker_snapshot %}
{{ config(
    target_schema='snapshots',
    unique_key='TickerKey',
    strategy='check',
    check_cols=['TickerSymbol','Exchange','Class','Currency']
) }}

SELECT
    TickerKey,
    TickerSymbol,
    Exchange,
    Class,
    Currency,
    Sector,
    _load_datetime
FROM {{ ref('stg_DimTicker') }}
{% endsnapshot %}