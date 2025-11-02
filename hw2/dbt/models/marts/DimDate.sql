
{{ config(
    materialized='table',
    schema='gold',
    order_by=['DateKey']
) }}

SELECT
    DateKey,
    TradingDate,
    Year,
    Month,
    Day,
    Quarter
FROM {{ ref('stg_DimDate') }}