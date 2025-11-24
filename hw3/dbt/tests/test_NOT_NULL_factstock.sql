SELECT *
FROM {{ ref('FactStock') }}
WHERE CompanyKey IS NULL
   OR TickerKey IS NULL
   OR TradingDate IS NULL