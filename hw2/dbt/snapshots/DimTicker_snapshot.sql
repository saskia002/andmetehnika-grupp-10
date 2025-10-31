{% snapshot DimTickersnapshot %}
{{ config(
    unique_key='TickerKey',
    strategy='check',
    check_cols=['TickerSymbol','Exchange','Class','Currency']
) }}

SELECT
    TicketSymbol,
    Exchange,
    Class,
    Currency
FROM {{ ref('DimTicker') }}
{% endsnapshot %}