
{{ config(
    materialized='table',
    schema='silver'
) }}

SELECT
    CAST(xxHash64(concat(company,headquarters)) AS UInt32) AS CompanyKey,  -- Primary Key
    CAST(company AS String) AS CompanyName,
    CAST(headquarters AS String) AS Headquarters,
    CAST(industry AS String) AS Industry,
    CAST('sector' AS String) AS Sector,  -- optional, can drop later if not needed
    CAST(_ingested_at AS DateTime) AS _load_datetime
FROM {{ source('bronze', 'companies_raw') }}
