
{{ config(
    materialized='table',
    schema='Forbes_2000'
) }}

SELECT
    *
FROM {{ ref('stg_DimDate') }}