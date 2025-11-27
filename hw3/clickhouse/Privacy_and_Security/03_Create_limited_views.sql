-- Create schema for limited views
CREATE DATABASE IF NOT EXISTS gold_limited_views;

CREATE OR REPLACE VIEW gold_limited_views.v_limited_DimCompany AS
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

CREATE OR REPLACE VIEW gold_limited_views.v_limited_DimDate AS
SELECT
    DateKey,
    TradingDate,
    Year,
    Month,
    Day,
    Quarter
FROM gold.DimDate;

CREATE OR REPLACE VIEW gold_limited_views.v_limited_DimTicker AS
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

CREATE OR REPLACE VIEW gold_limited_views.v_limited_FactFinancials AS
SELECT
    CompanyKey,
    Year,
    Sales,
    Profit,
    Assets,
    MarketValue
FROM gold.FactFinancials;

CREATE OR REPLACE VIEW gold_limited_views.v_limited_FactForbesRank AS
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

CREATE OR REPLACE VIEW gold_limited_views.v_limited_FactStock AS
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
