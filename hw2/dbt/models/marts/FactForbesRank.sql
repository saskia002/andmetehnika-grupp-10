
{{ config(
    materialized='table',
    schema='gold'
) }}

SELECT
    *
FROM {{ ref('stg_FactForbesRank') }}