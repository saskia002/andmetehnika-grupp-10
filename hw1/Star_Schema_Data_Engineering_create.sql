-- Created by Vertabelo (http://vertabelo.com)
-- Last modification date: 2025-10-03 06:39:39.168

-- tables
-- Table: DimCompany
CREATE TABLE DimCompany (
    CompanyKey serial  NOT NULL,
    CompanyName text  NOT NULL,
    ForbesRank int  NOT NULL,
    Headquarters text  NOT NULL,
    Industry text  NOT NULL,
    Sector text  NOT NULL,
    Sales money  NOT NULL,
    Profit money  NOT NULL,
    Assets money  NOT NULL,
    MarketValue money  NOT NULL,
    CONSTRAINT DimCompany_pk PRIMARY KEY (CompanyKey)
);

-- Table: DimDate
CREATE TABLE DimDate (
    DateKey serial  NOT NULL,
    TradingDay date  NOT NULL,
    Year int  NOT NULL,
    Month int  NOT NULL,
    Day int  NOT NULL,
    CONSTRAINT DimDate_pk PRIMARY KEY (DateKey)
);

-- Table: DimTicker
CREATE TABLE DimTicker (
    TickerKey serial  NOT NULL,
    TickerSymbol text  NOT NULL,
    Exchange text  NOT NULL,
    Class text  NOT NULL,
    Currency text  NOT NULL,
    CONSTRAINT DimTicker_pk PRIMARY KEY (TickerKey)
);

-- Table: FactStock
CREATE TABLE FactStock (
    DateKey serial  NOT NULL,
    CompanyKey int  NOT NULL,
    TickerKey int  NOT NULL,
    OpenPrice money  NOT NULL,
    ClosePrice money  NOT NULL,
    HighPrice money  NOT NULL,
    LowPrice money  NOT NULL,
    MarketCap money  NOT NULL,
    Dividend boolean  NOT NULL
);

-- foreign keys
-- Reference: DimCompany_FactStockPrice (table: FactStock)
ALTER TABLE FactStock ADD CONSTRAINT DimCompany_FactStockPrice
    FOREIGN KEY (CompanyKey)
    REFERENCES DimCompany (CompanyKey)  
    NOT DEFERRABLE 
    INITIALLY IMMEDIATE
;

-- Reference: DimDate_FactStockPrice (table: FactStock)
ALTER TABLE FactStock ADD CONSTRAINT DimDate_FactStockPrice
    FOREIGN KEY (DateKey)
    REFERENCES DimDate (DateKey)  
    NOT DEFERRABLE 
    INITIALLY IMMEDIATE
;

-- Reference: DimTicker_FactStockPrice (table: FactStock)
ALTER TABLE FactStock ADD CONSTRAINT DimTicker_FactStockPrice
    FOREIGN KEY (TickerKey)
    REFERENCES DimTicker (TickerKey)  
    NOT DEFERRABLE 
    INITIALLY IMMEDIATE
;

-- End of file.

