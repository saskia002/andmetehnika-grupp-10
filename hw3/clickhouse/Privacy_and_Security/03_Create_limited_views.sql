-- Create schema for limited views
CREATE DATABASE IF NOT EXISTS gold_limited_views;

DROP TABLE IF EXISTS gold_limited_views.v_limited_DimCompany;
CREATE TABLE gold_limited_views.v_limited_DimCompany
ENGINE = MergeTree()
ORDER BY CompanyKey
AS
SELECT
    CompanyKey,    
    concat(substring(CompanyName, 1, 3), '***') AS CompanyName_masked, -- Show first 3 chars + ***
    Headquarters,
    CASE
        WHEN Industry_count < 10 THEN 'Other'
        ELSE Industry
    END AS Industry_masked,
    Sector,
    ValidFrom,    
    ValidTo       
FROM
(
    SELECT
        CompanyKey,
        CompanyName,
        Headquarters,
        Industry,
        Sector,
        ValidFrom,
        ValidTo,
        COUNT(*) OVER (PARTITION BY Industry) AS Industry_count
    FROM gold.DimCompany
) t;

DROP TABLE IF EXISTS gold_limited_views.v_limited_DimDate;
CREATE TABLE gold_limited_views.v_limited_DimDate
ENGINE = MergeTree()
ORDER BY DateKey
AS
SELECT
    DateKey,
    TradingDate,
    Year,
    Month,
    Day,
    Quarter
FROM gold.DimDate;

DROP TABLE IF EXISTS gold_limited_views.v_limited_DimTicker;
CREATE TABLE gold_limited_views.v_limited_DimTicker
ENGINE = MergeTree()
ORDER BY TickerKey
AS
SELECT
    TickerKey,
    TickerSymbol,
    Exchange,
    Class,
    Currency,
    Sector,
    ValidFrom, 
    ValidTo      
FROM gold.DimTicker;

DROP TABLE IF EXISTS gold_limited_views.v_limited_FactFinancials;
CREATE TABLE gold_limited_views.v_limited_FactFinancials
ENGINE = MergeTree()
ORDER BY (CompanyKey, Year)
AS
SELECT
    CompanyKey,
    Year,
    Sales,
    Profit,
    Assets,
    MarketValue
FROM gold.FactFinancials;

DROP TABLE IF EXISTS gold_limited_views.v_limited_FactForbesRank;
CREATE TABLE gold_limited_views.v_limited_FactForbesRank
ENGINE = MergeTree()
ORDER BY (CompanyKey, Year)
AS
SELECT
    ForbesRankKey,
    CompanyKey,
    Year,
    concat(
        toString(intDiv(toUInt64(ForbesRank), 100) * 100),
        '–',
        toString(intDiv(toUInt64(ForbesRank), 100) * 100 + 99)
    ) AS ForbesRank_range -- MASKED COLUMN as range
FROM gold.FactForbesRank;

DROP TABLE IF EXISTS gold_limited_views.v_limited_FactStock;
CREATE TABLE gold_limited_views.v_limited_FactStock
ENGINE = MergeTree()
ORDER BY (DateKey, CompanyKey, TickerKey)
AS
SELECT
    DateKey,
    CompanyKey,
    TickerKey,
    TradingDate,
    OpenPrice,
    ClosePrice,
    HighPrice,
    LowPrice,
    MarketCap,
    Dividend
FROM gold.FactStock;