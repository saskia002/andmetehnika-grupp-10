{{ config(
    materialized='table',
    schema='silver'
) }}

SELECT
    -- Foreign key for star schema joins
    CAST(xxHash64(concat(company,headquarters)) AS UInt32) AS CompanyKey,
    -- Fact measures
    CAST(toUInt32(financial_year) AS UInt32) AS Year,
    CAST(sales_in_millions AS UInt64) AS Sales,
    CAST(profit_in_millions AS Int64) AS Profit,  -- profit can be negative
    CAST(assets_in_millions AS UInt64) AS Assets,
    CAST(market_value_in_millions AS UInt64) AS MarketValue
FROM bronze.companies_raw_view
WHERE company IS NOT NULL
