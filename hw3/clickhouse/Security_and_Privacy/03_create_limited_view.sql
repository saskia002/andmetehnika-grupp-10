CREATE OR REPLACE VIEW gold.v_limited_DimCompany AS
SELECT
    CompanyKey,  
    CompanyName,  
    concat('***', left(CompanyName, len(CompanyName)-5)) AS CompanyName_masked,
    Headquarters,
    CASE
        WHEN Industry_count < 10 THEN 'Other'
        ELSE Industry
    END AS Industry_masked -- Mask as OTHER when less than 10 companies with that Industry
    Sector,
    ValidFrom,    
    ValidTo       
FROM
(
    SELECT
        CompanyName,
        Industry,
        COUNT(*) OVER (PARTITION BY Industry) AS Industry_count
    FROM gold.DimCompany
) t

CREATE OR REPLACE VIEW gold.v_limited_DimDate AS
SELECT
    DateKey,
    TradingDate,
    Year,
    Month,
    Day,
    Quarter
FROM gold.DimDate 

CREATE OR REPLACE VIEW gold.v_limited_DimTicker AS
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

CREATE OR REPLACE VIEW gold.v_limited_FactFinancials AS
SELECT
    CompanyKey,
    Year,
    Sales,
    Profit,
    Assets,
    MarketValue
FROM gold.FactFinancials

CREATE OR REPLACE VIEW gold.v_limited_FactForbesRank AS
SELECT
    ForbesRankKey,
    CompanyKey,
    Year,
    concat(
        toString(intDiv(toUInt64(ForbesRank), 1000) * 100),
        '–',
        toString(intDiv(toUInt64(ForbesRank), 1000) * 100 + 99)
    ) AS ForbesRank_range, -- MASKED COLUMN as range
FROM gold.FactForbesRank

CREATE OR REPLACE VIEW gold.v_limited_FactStock AS
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