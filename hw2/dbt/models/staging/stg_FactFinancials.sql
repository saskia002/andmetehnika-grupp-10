
SELECT
    abs(mod(cast(hash(company) as bigint), 1000000000)) AS CompanyKey,  -- primary key
    year AS Year,                          -- currently only 2024 data in source, but later we could load other years as well
    sales_in_millions AS Sales,
    profit_in_millions AS Profit,
    assets_in_millions AS Assets,
    market_value_in_millions AS MarketValue
FROM bronze.companies_raw;