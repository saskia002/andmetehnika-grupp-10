-- Demo Queries based on business questions in the business brief
-- Updated for hw2 schema structure (ClickHouse)
-- Note: Schema uses CompanyKey for companies (company+headquarters) and TickerKey for tickers (ticker_symbol+exchange)
-- FactStock uses TickerKey for joins, while FactFinancials and FactForbesRank use CompanyKey

--Valuation

--Which industries have the highest market capitalization among top 20 rank using a 3 month rolling average?

WITH RollingAvg AS (
    SELECT
        dc.Industry AS Industry,
        dd.Year AS Year,
        dd.Month AS Month,
        AVG(fs.MarketCap) OVER (
            PARTITION BY dc.Industry
            ORDER BY dd.Year, dd.Month
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ) AS avg_3m_marketcap
    FROM gold.FactStock fs
    JOIN gold.DimTicker dt ON fs.TickerKey = dt.TickerKey
    JOIN gold.DimDate dd ON fs.DateKey = dd.DateKey
    JOIN bronze.companies_raw_view cr ON dt.TickerSymbol = cr.ticker
    JOIN gold.DimCompany dc ON CAST(xxHash64(concat(cr.company, cr.headquarters)) AS UInt32) = dc.CompanyKey
    JOIN gold.FactForbesRank fr ON dc.CompanyKey = fr.CompanyKey
    WHERE fr.ForbesRank <= 20
)
SELECT 
    ra.Industry, 
    ra.Year, 
    ra.Month, 
    ra.avg_3m_marketcap
FROM RollingAvg ra
ORDER BY ra.avg_3m_marketcap DESC
LIMIT 10;

--What is a company's valuation relative to its earnings performance. Name top 10 stocks per P/E (price to earnings) ratio?

SELECT 
    dc.CompanyName,
    dc.Industry,
    AVG(fs.ClosePrice) / NULLIF(AVG(ff.Profit), 0) AS pe_ratio
FROM gold.FactStock fs
JOIN gold.DimTicker dt ON fs.TickerKey = dt.TickerKey
JOIN bronze.companies_raw cr ON dt.TickerSymbol = cr.ticker
JOIN gold.DimCompany dc ON CAST(xxHash64(concat(cr.company, cr.headquarters)) AS UInt32) = dc.CompanyKey
JOIN gold.FactFinancials ff ON dc.CompanyKey = ff.CompanyKey
GROUP BY dc.CompanyName, dc.Industry
HAVING AVG(ff.Profit) > 0
ORDER BY pe_ratio DESC
LIMIT 10;

--Higher ROA indicates how well a company is converting its assets into new income. Which companies or industries are most efficient based on  their ROA (name top 10)?

SELECT 
    dc.CompanyName,
    dc.Industry,
    (CAST(ff.Profit AS Float64) / NULLIF(CAST(ff.Assets AS Float64), 0)) AS roa
FROM gold.DimCompany dc
JOIN gold.FactFinancials ff ON dc.CompanyKey = ff.CompanyKey
WHERE ff.Assets > 0
ORDER BY roa DESC
LIMIT 10;

--Is the company over or undervalued compared to its peers? List for each industry the average P/E using only Forbes 2000 data and select the top company for each sector based on P/E including the delta with the average of that industry.

WITH IndustryPE AS (
    SELECT 
        dc.Industry,
        AVG(fs.ClosePrice / NULLIF(ff.Profit, 0)) AS avg_pe
    FROM gold.FactStock fs
    JOIN gold.DimTicker dt ON fs.TickerKey = dt.TickerKey
    JOIN bronze.companies_raw cr ON dt.TickerSymbol = cr.ticker
    JOIN gold.DimCompany dc ON CAST(xxHash64(concat(cr.company, cr.headquarters)) AS UInt32) = dc.CompanyKey
    JOIN gold.FactFinancials ff ON dc.CompanyKey = ff.CompanyKey
    JOIN gold.FactForbesRank fr ON dc.CompanyKey = fr.CompanyKey
    WHERE fr.ForbesRank <= 2000 AND ff.Profit > 0
    GROUP BY dc.Industry
),
CompanyPE AS (
    SELECT 
        dc.Industry,
        dc.CompanyName,
        AVG(fs.ClosePrice / NULLIF(ff.Profit, 0)) AS company_pe
    FROM gold.FactStock fs
    JOIN gold.DimTicker dt ON fs.TickerKey = dt.TickerKey
    JOIN bronze.companies_raw cr ON dt.TickerSymbol = cr.ticker
    JOIN gold.DimCompany dc ON CAST(xxHash64(concat(cr.company, cr.headquarters)) AS UInt32) = dc.CompanyKey
    JOIN gold.FactFinancials ff ON dc.CompanyKey = ff.CompanyKey
    WHERE ff.Profit > 0
    GROUP BY dc.Industry, dc.CompanyName
)
SELECT 
    cp.Industry,
    cp.CompanyName,
    cp.company_pe,
    ip.avg_pe,
    cp.company_pe - ip.avg_pe AS delta
FROM CompanyPE cp
JOIN IndustryPE ip ON cp.Industry = ip.Industry
ORDER BY cp.Industry, delta DESC;

--Profitability

--Which industries generate the highest total revenue among Forbes 2000 companies? Name top 20.

SELECT 
    dc.Industry,
    SUM(ff.Sales) AS total_revenue
FROM gold.DimCompany dc
JOIN gold.FactFinancials ff ON dc.CompanyKey = ff.CompanyKey
JOIN gold.FactForbesRank fr ON dc.CompanyKey = fr.CompanyKey
WHERE fr.ForbesRank <= 2000
GROUP BY dc.Industry
ORDER BY total_revenue DESC
LIMIT 20;

--Which industry among Forbes 2000 has had the highest annual increase in stock market price based on average y-o-y growth of closing price?

/* TBD */
