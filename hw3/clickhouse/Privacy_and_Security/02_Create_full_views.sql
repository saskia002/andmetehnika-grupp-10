-- Create schema for limited views
CREATE DATABASE IF NOT EXISTS gold_full_views;

CREATE OR REPLACE VIEW gold_full_views.v_DimCompany AS
SELECT
    CompanyKey,  
    CompanyName,  
    Headquarters,
    Industry,
    Sector,
    ValidFrom,    
    ValidTo       
FROM gold.DimCompany;

CREATE OR REPLACE VIEW gold_full_views.v_DimDate AS
SELECT
    DateKey,
    TradingDate,
    Year,
    Month,
    Day,
    Quarter
FROM gold.DimDate;

CREATE OR REPLACE VIEW gold_full_views.v_DimTicker AS
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

CREATE OR REPLACE VIEW gold_full_views.v_FactFinancials AS
SELECT
    CompanyKey,
    Year,
    Sales,
    Profit,
    Assets,
    MarketValue
FROM gold.FactFinancials;

CREATE OR REPLACE VIEW gold_full_views.v_FactForbesRank AS
SELECT
    ForbesRankKey,
    CompanyKey,
    Year,
    ForbesRank
FROM gold.FactForbesRank;

CREATE OR REPLACE VIEW gold_full_views.v_FactStock AS
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