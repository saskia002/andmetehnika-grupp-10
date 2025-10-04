-- Demo Queries based on business questions in the business brief

--Valuation

--Which industries have the highest market capitalization among top 20 rank using a 3 month rolling average?

WITH RollingAvg AS (
    SELECT
        c.Industry,
        d.Year,
        d.Month,
        AVG(f.MarketCap) OVER (
            PARTITION BY c.Industry
            ORDER BY d.Year, d.Month
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ) AS avg_3m_marketcap
    FROM FactStock f
    JOIN DimCompany c ON f.CompanyKey = c.CompanyKey
    JOIN DimDate d ON f.DateKey = d.DateKey
    WHERE c.ForbesRank <= 20
)
SELECT Industry, Year, Month, avg_3m_marketcap
FROM RollingAvg
ORDER BY avg_3m_marketcap DESC
LIMIT 10;

--What is a company's valuation relative to its earnings performance. Name top 10 stocks per P/E (price to earnings) ratio?

SELECT 
    c.CompanyName,
    c.Industry,
    AVG(f.ClosePrice) / NULLIF(AVG(c.Profit), 0) AS pe_ratio
FROM FactStock f
JOIN DimCompany c ON f.CompanyKey = c.CompanyKey
GROUP BY c.CompanyName, c.Industry
HAVING AVG(c.Profit) > 0
ORDER BY pe_ratio DESC
LIMIT 10;

--Higher ROA indicates how well a company is converting its assets into new income. Which companies or industries are most efficient based on  their ROA (name top 10)?

SELECT 
    c.CompanyName,
    c.Industry,
    (c.Profit::numeric / NULLIF(c.Assets, 0)) AS roa
FROM DimCompany c
WHERE c.Assets > 0
ORDER BY roa DESC
LIMIT 10;

--Is the company over or undervalued compared to its peers? List for each industry the average P/E using only Forbes 2000 data and select the top company for each sector based on P/E including the delta with the average of that industry.

WITH IndustryPE AS (
    SELECT 
        c.Industry,
        AVG(f.ClosePrice / NULLIF(c.Profit, 0)) AS avg_pe
    FROM FactStock f
    JOIN DimCompany c ON f.CompanyKey = c.CompanyKey
    WHERE c.ForbesRank <= 2000
    GROUP BY c.Industry
)
SELECT 
    c.Industry,
    c.CompanyName,
    (AVG(f.ClosePrice) / NULLIF(AVG(c.Profit), 0)) AS company_pe,
    i.avg_pe,
    (AVG(f.ClosePrice) / NULLIF(AVG(c.Profit), 0)) - i.avg_pe AS delta
FROM FactStock f
JOIN DimCompany c ON f.CompanyKey = c.CompanyKey
JOIN IndustryPE i ON c.Industry = i.Industry
GROUP BY c.Industry, c.CompanyName, i.avg_pe
ORDER BY delta DESC;

--Profitability

--Which industries generate the highest total revenue among Forbes 2000 companies? Name top 20.

SELECT 
    c.Industry,
    SUM(c.Sales) AS total_revenue
FROM DimCompany c
WHERE c.ForbesRank <= 2000
GROUP BY c.Industry
ORDER BY total_revenue DESC
LIMIT 20;

--Which industry among Forbes 2000 has had the highest annual increase in stock market price based on average y-o-y growth of closing price?

WITH YearlyAvg AS (
    SELECT
        c.Industry,
        d.Year,
        AVG(f.ClosePrice) AS avg_close
    FROM FactStock f
    JOIN DimCompany c ON f.CompanyKey = c.CompanyKey
    JOIN DimDate d ON f.DateKey = d.DateKey
    WHERE c.ForbesRank <= 2000
    GROUP BY c.Industry, d.Year
),
YoYGrowth AS (
    SELECT
        Industry,
        Year,
        (avg_close - LAG(avg_close) OVER (PARTITION BY Industry ORDER BY Year)) / 
        NULLIF(LAG(avg_close) OVER (PARTITION BY Industry ORDER BY Year), 0) AS yoy_growth
    FROM YearlyAvg
)
SELECT Industry, AVG(yoy_growth) AS avg_yoy_growth
FROM YoYGrowth
GROUP BY Industry
ORDER BY avg_yoy_growth DESC
LIMIT 1;
