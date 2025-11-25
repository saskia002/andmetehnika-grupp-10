
{{ config(
    materialized='table',
    schema='gold',
    order_by=['CompanyKey', 'Year'] 
) }}

SELECT
    CompanyKey,
    Year,
    Sales,
    Profit,
    Assets,
    MarketValue
FROM {{ ref('stg_FactFinancials') }}