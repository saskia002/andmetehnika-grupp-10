

{{ config(
    materialized='table',
    schema='silver'
) }}

SELECT
    CAST(xxHash64(rank) AS UInt32) AS ForbesRankKey,
    CAST(xxHash64(concat(company,headquarters)) AS UInt32) AS CompanyKey,
    CAST(toUInt32(toYear(now())) AS UInt32) AS Year, --TO BE REPLACED
    CAST(rank AS UInt32) AS ForbesRank
FROM {{ source('bronze', 'companies_raw') }}
WHERE company IS NOT NULL
