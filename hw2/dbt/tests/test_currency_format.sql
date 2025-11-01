SELECT *
FROM {{ ref('DimTicker') }}
WHERE length(Currency) != 3
   OR Currency IS NULL