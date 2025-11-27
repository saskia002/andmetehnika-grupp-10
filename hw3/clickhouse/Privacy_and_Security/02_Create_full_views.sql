-- Create the database
CREATE DATABASE IF NOT EXISTS gold_full_views;

-- Drop and create tables (not views)
DROP TABLE IF EXISTS gold_full_views.v_DimCompany;
CREATE TABLE gold_full_views.v_DimCompany
ENGINE = MergeTree()
ORDER BY CompanyKey
AS
SELECT
    CompanyKey,  
    CompanyName,  
    Headquarters,
    Industry,
    Sector,
    ValidFrom,    
    ValidTo       
FROM gold.DimCompany;

DROP TABLE IF EXISTS gold_full_views.v_DimDate;
CREATE TABLE gold_full_views.v_DimDate
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

DROP TABLE IF EXISTS gold_full_views.v_DimTicker;
CREATE TABLE gold_full_views.v_DimTicker
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

DROP TABLE IF EXISTS gold_full_views.v_FactFinancials;
CREATE TABLE gold_full_views.v_FactFinancials
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

DROP TABLE IF EXISTS gold_full_views.v_FactForbesRank;
CREATE TABLE gold_full_views.v_FactForbesRank
ENGINE = MergeTree()
ORDER BY (CompanyKey, Year)
AS
SELECT
    ForbesRankKey,
    CompanyKey,
    Year,
    ForbesRank
FROM gold.FactForbesRank;

DROP TABLE IF EXISTS gold_full_views.v_FactStock;
CREATE TABLE gold_full_views.v_FactStock
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