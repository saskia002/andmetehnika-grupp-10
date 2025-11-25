
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
    CAST(now() AS DateTime) AS _load_datetime  -- Using current timestamp since duplicates are prevented in ETL
FROM bronze.companies_raw_view
WHERE company IS NOT NULL AND headquarters IS NOT NULL
