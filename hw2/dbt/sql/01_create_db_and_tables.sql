-- DROP DATABASE IF EXISTS Forbes_2000;

CREATE DATABASE IF NOT EXISTS Forbes_2000;

-- ========== Dimensions ==========
CREATE TABLE Forbes_2000.DimDate(
    DateKey     UInt32, -- Primary key
    TradingDate Date,
    Year        UInt32,
    Month       UInt32, 
    Day         UInt32,
    Quarter     UInt32  -- NEW FIELD compared to PR1 - values would be 1, 2, 3, 4. we added this so we could merge with new fact table FactFinancials
) ENGINE = MergeTree
ORDER BY (DateKey);

-- SCD2-friendly dimension: 
-- We changed DimCompany to SCD type 2 because company can change name, merge, be acquired by another company, etc
-- When Company changes name like Facebook to META then they can request to change also Ticket in the stock market. 
-- Latter change would need to be approved by Stock Exchange

-- Storing basic Company related data - ONE field ForbesRank removed and moved to separate FACTTABLE
CREATE TABLE Forbes_2000.DimCompany (
    CompanyKey      UInt32,  -- Primary Key - should not change whatever happens to company
    CompanyName     String,  
    Headquarters    String,
    Industry        String,
    Sector          String,
    ValidFrom       Date,   -- NEW FIELD compared to PR1: as we changes DimCompany to SDC2 then I add ValidFrom and ValidTo dates
    ValidTo         Date    -- NEW FIELD compares to PR1
) ENGINE = MergeTree
ORDER BY (CompanyKey);

-- Storing Ticker dimension
CREATE TABLE Forbes_2000.DimTicker (
    TickerKey       UInt32, -- Primary Key
    TickerSymbol    String,
    Exchange        String,
    Class           String,
    Currency        String,
    ValidFrom       Date,   -- NEW FIELD compared to PR1: as we changes DimCompany to SDC2 then I add ValidFrom and ValidTo dates
    ValidTo         Date    -- NEW FIELD compares to PR1
) ENGINE = MergeTree
ORDER BY (TickerKey);

-- ========== Fact ==========
-- Denormalize FullDate onto fact for partitioning and fast time filtering.
CREATE TABLE Forbes_2000.FactFinancials (
    CompanyKey      UInt64, -- Foreign key
    Year            UInt32, -- Foreign key - currently we have only 2025 because we use as data source Kaggle and it has only 2025 currently, but theoretically our schema allows to load also other values
    Sales           UInt64,
    Profit          Int64,  -- this can also be negative value and thus UInt64 is not suitable as it allows only positive values
    Assets          UInt64,
    MarketValue     UInt64
) ENGINE = MergeTree
PARTITION BY Year
ORDER BY (CompanyKey, Year);

-- We removed financial data from DimCompany and added it into second fact table to be able to see quarterly changes. 
-- It is linked to dimcompany table that has now validfrom and validto
-- thus when sector changes or name changes we can still properly report historical data
CREATE TABLE Forbes_2000.FactStock (
    CompanyKey      UInt64, -- Foreign key
    DateKey         UInt32, -- Foreign key
    TickerKey       UInt32, -- Foreign key
    OpenPrice       Float64,
    ClosePrice      Float64,
    HighPrice       Float64,
    LowPrice        Float64,
    MarketCap       UInt64,
    Dividend        Float64  -- we changed from boolean to float as we are a loading last divident amount paid 
) ENGINE = MergeTree
PARTITION BY toYYYYMM(FullDate)
ORDER BY (FullDate, CompanyKey, TickerKey);

CREATE TABLE Forbes_2000.FactForbesRank (
    ForbesRankKey   UInt32, -- Primary key 
    CompanyKey      UInt32, -- Foreign key - should not change whatever happens to company
    Year            UInt32,
    ForbesRank      UInt32
) ENGINE = MergeTree
PARTITION BY Year
ORDER BY (ForbesRankKey, Year);