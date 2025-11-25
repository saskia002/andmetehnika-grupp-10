

{{ config(
    materialized='table',
    schema='silver'
) }}

SELECT
    CAST(xxHash64(rank) AS UInt32) AS ForbesRankKey,
    CAST(xxHash64(concat(company,headquarters)) AS UInt32) AS CompanyKey,
    CAST(toUInt32(financial_year) AS UInt32) AS Year,  -- Using financial_year from seed data
    CAST(rank AS UInt32) AS ForbesRank
FROM bronze.companies_raw_view
WHERE company IS NOT NULL
