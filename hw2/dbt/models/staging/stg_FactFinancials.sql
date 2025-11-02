

{{ config(
    materialized='table',
    schema='silver'
) }}

SELECT
    CAST(xxHash64(concat(company,headquarters)) AS UInt32)  AS CompanyKey,  -- surrogate primary key
    CAST(toUInt32(financial_year) AS UInt32) AS Year,                                                      -- year as UInt32
    CAST(sales_in_millions AS UInt64) AS Sales,                                         -- sales as UInt64
    CAST(profit_in_millions AS Int64) AS Profit,                                        -- profit can be negative
    CAST(assets_in_millions AS UInt64) AS Assets,
    CAST(market_value_in_millions AS UInt64) AS MarketValue
FROM {{ source('bronze', 'companies_raw') }}
WHERE company IS NOT NULL
