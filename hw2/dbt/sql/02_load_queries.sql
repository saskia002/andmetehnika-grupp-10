-- ========== Dimensions ==========

-- ========== Dimensions ==========

INSERT INTO Forbes_2000.DimDate
SELECT toUInt32(DateKey), toDate(TradingDate), toUInt32(Year), toUInt32(Month), toUInt32(Day), toUInt32(Quarter)
FROM file('dim_date.csv', 'CSVWithNames');

INSERT INTO Forbes_2000.DimCompany
SELECT toUInt32(CompanyKey), toString(CompanyName), toString(Headquarters), toString(Industry), toString(Sector),
       toDate(ValidFrom), toDate(ValidTo)
FROM file('dim_company.csv', 'CSVWithNames');

INSERT INTO Forbes_2000.DimTicker
SELECT toUInt32(TickerKey), toString(TickerSymbol), toString(Exchange), toString(Class), toString(Currency),
       toDate(ValidFrom), toDate(ValidTo)
FROM file('dim_ticker.csv', 'CSVWithNames');

-- ========== Fact Tables ==========

INSERT INTO Forbes_2000.FactFinancials
SELECT toUInt64(CompanyKey), toUInt32(Year), toUInt64(Sales), toUInt64(Profit),
       toUInt64(Assets), toUInt64(MarketValue)
FROM file('fact_financials.csv', 'CSVWithNames');

INSERT INTO Forbes_2000.FactStock
SELECT toUInt64(CompanyKey), toUInt32(DateKey), toUInt32(TickerKey), toUInt32(OpenPrice),
       toUInt32(ClosePrice), toUInt32(HighPrice), toUInt32(LowPrice),
       toUInt64(MarketCap), toUInt8(Dividend)
FROM file('fact_stock.csv', 'CSVWithNames');

INSERT INTO Forbes_2000.FactForbesRank
SELECT toUInt32(ForbesRankKey), toUInt32(CompanyKey), toUInt32(Year), toUInt32(ForbesRank)
FROM file('fact_forbesrank.csv', 'CSVWithNames');

-- ========== Quick verification ==========
SELECT 'DimDate' AS table_name, count() AS rows FROM Forbes_2000.DimDate
UNION ALL SELECT 'DimCompany', count() FROM Forbes_2000.DimCompany
UNION ALL SELECT 'DimTicker', count() FROM Forbes_2000.DimTicker
UNION ALL SELECT 'FactFinancials', count() FROM Forbes_2000.FactFinancials
UNION ALL SELECT 'FactStock', count() FROM Forbes_2000.FactStock
UNION ALL SELECT 'FactForbesRank', count() FROM Forbes_2000.FactForbesRank;

