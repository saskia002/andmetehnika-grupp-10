SELECT *
FROM {{ ref('FactStock') }}
WHERE TradingDate >= today()