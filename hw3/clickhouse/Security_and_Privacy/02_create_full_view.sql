CREATE OR REPLACE VIEW gold.v_DimCompany AS
SELECT
    CompanyKey,  
    CompanyName,  
    Headquarters,
    Industry,
    Sector,
    ValidFrom,    
    ValidTo       
FROM gold.DimCompany;

CREATE OR REPLACE VIEW gold.v_DimDate AS
SELECT
    DateKey,
    TradingDate,
    Year,
    Month,
    Day,
    Quarter
FROM gold.DimDate 

CREATE OR REPLACE VIEW gold.v_DimTicker AS
SELECT
    TickerKey, 
    TickerSymbol,
    Exchange,
    Class,
    Currency,
    Sector,
    ValidFrom, 
    ValidTo      
FROM gold.DimTicker

CREATE OR REPLACE VIEW gold.v_FactFinancials AS
SELECT
    CompanyKey,
    Year,
    Sales,
    Profit,
    Assets,
    MarketValue
FROM gold.FactFinancials

CREATE OR REPLACE VIEW gold.v_FactForbesRank AS
SELECT
    ForbesRankKey,
    CompanyKey,
    Year,
    ForbesRank
FROM gold.FactForbesRank

CREATE OR REPLACE VIEW gold.v_FactStock AS
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
FROM gold.FactStock