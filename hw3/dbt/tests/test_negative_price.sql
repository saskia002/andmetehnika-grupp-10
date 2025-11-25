SELECT *
FROM {{ ref('FactStock') }}
WHERE OpenPrice <= 0
    OR ClosePrice <= 0
    OR HighPrice <= 0
    OR LowPrice <= 0
    OR MarketCap <= 0 
    OR Dividend <= 0