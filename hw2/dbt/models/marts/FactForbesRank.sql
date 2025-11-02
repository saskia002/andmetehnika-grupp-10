
{{ config(
    materialized='table',
    schema='gold',
    order_by=['CompanyKey', 'Year'] 
) }}

SELECT
    ForbesRankKey,
    CompanyKey,
    Year,
    ForbesRank
FROM {{ ref('stg_FactForbesRank') }}