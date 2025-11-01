SELECT *
FROM {{ ref('DimTicker') }}
WHERE TickerKey IS NULL
   OR Currency IS NULL