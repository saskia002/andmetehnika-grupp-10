## Project setup

## Running the Project:

1. Clone the repository to your computer

2. Open the project folder in your terminal

3. Go inside the folder 'hw2'

    ```bash
    cd hw2
    ```

4. Run docker

    ```bash
    docker-compose up -d
    ```

5. Airflow is scheduled to run every day at 22:00 UTC, to run it manually:

    5.1. (Optional) Firstly pulling stock info with yfinance API takes time (~15 minutes), to shorten it you can pull less data

        5.1.1. Uncomment this line in plugins/tasks/fetch_yfinance.py file inside the fetch_stock_info() function

            ```bash
            df = df.iloc[:250]
            ```
        
    5.2. Open Airflow on http://localhost:8080/home

    5.3. Log into Airflow 

        username: airflow

        password: airflow
    
    5.4. Trigger the DAG called load_companies_dag - it will trigger fetch_yfinance_dag after completion

    5.5. Wait for the DAGs to be finished and ...

## Airflow

Daily at 22:00 UTC
```
┌─────────────────────────────┐
│     load_companies_dag      │
│  (Loads data from MongoDB)  │
└─────────────┬───────────────┘
              │ 
              │ Trigger next DAG
              │ with data from MongoDB
              │ 
              ▼
┌──────────────────────────────┐
│      fetch_yfinance_dag      │
│  (Fetch data from yfinance,  │
│  loads data into ClickHouse) │
└──────────────────────────────┘
```

## Example analytical queries

**1. Which industries have the highest market capitalization among top 20 rank using a 3 month rolling average?**
```
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
```
```

    ┌─Industry────────────────────────┬─Year─┬─Month─┬───avg_3m_marketcap─┐
 1. │ Technology Hardware & Equipment │ 2025 │    11 │    700058194411520 │
 2. │ Technology Hardware & Equipment │ 2025 │    11 │    700058194411520 │
 3. │ Technology Hardware & Equipment │ 2025 │    11 │  468037157082453.3 │
 4. │ Technology Hardware & Equipment │ 2025 │    11 │ 236016119753386.66 │
 5. │ IT Software & Services          │ 2025 │    11 │      3848559656960 │
 6. │ IT Software & Services          │ 2025 │    11 │      3848559656960 │
 7. │ IT Software & Services          │ 2025 │    11 │ 3110434461013.3335 │
 8. │ Banking                         │ 2025 │    11 │      2814211784704 │
 9. │ Retail and Wholesale            │ 2025 │    11 │      2610764447744 │
10. │ Retail and Wholesale            │ 2025 │    11 │      2610764447744 │
    └─────────────────────────────────┴──────┴───────┴────────────────────┘
```

**2. What is a company's valuation relative to its earnings performance. Name top 10 stocks per P/E (price to earnings) ratio?**
```
SELECT 
    dd.Year,
    dd.Month,
    dd.Day,
    dc.CompanyName,
    dc.Industry,
    AVG(fs.ClosePrice) / NULLIF(AVG(ff.Profit), 0) AS pe_ratio
FROM gold.FactStock fs
JOIN gold.DimTicker dt ON fs.TickerKey = dt.TickerKey
JOIN bronze.companies_raw cr ON dt.TickerSymbol = cr.ticker
JOIN gold.DimCompany dc ON CAST(xxHash64(concat(cr.company, cr.headquarters)) AS UInt32) = dc.CompanyKey
JOIN gold.FactFinancials ff ON dc.CompanyKey = ff.CompanyKey
JOIN gold.DimDate dd ON fs.DateKey = dd.DateKey
GROUP BY dd.Year, dd.Month, dd.Day, dc.CompanyName, dc.Industry
HAVING AVG(ff.Profit) > 0
ORDER BY pe_ratio DESC
LIMIT 10;
```
```
    ┌─dd.Year─┬─Month─┬─Day─┬─CompanyName───────────────────────────────┬─Industry────────────────────────┬───────────pe_ratio─┐
 1. │    2025 │    11 │   2 │ Samsung SDI                               │ Capital Goods                   │ 4120.2531645569625 │
 2. │    2025 │    11 │   2 │ DCI Indonesia                             │ Business Services & Supplies    │ 3850.7462686567164 │
 3. │    2025 │    11 │   2 │ CJ Corporation                            │ Food, Drink & Tobacco           │               2000 │
 4. │    2025 │    11 │   2 │ LS Corp                                   │ Capital Goods                   │               1250 │
 5. │    2025 │    11 │   2 │ LG Innotek Co.,                           │ Technology Hardware & Equipment │  804.5774647887324 │
 6. │    2025 │    11 │   2 │ Vingroup                                  │ Construction                    │ 453.55555555555554 │
 7. │    2025 │    11 │   2 │ Grupo Bolivar                             │ Banking                         │  446.7724867724868 │
 8. │    2025 │    11 │   2 │ Korea Shipbuilding & Offshore Engineering │ Capital Goods                   │ 404.82456140350877 │
 9. │    2025 │    11 │   2 │ Hanwha Ocean                              │ Capital Goods                   │  381.0810810810811 │
10. │    2025 │    11 │   2 │ Krafton                                   │ IT Software & Services          │  283.1765935214211 │
    └─────────┴───────┴─────┴───────────────────────────────────────────┴─────────────────────────────────┴────────────────────┘
```

**3. Higher ROA indicates how well a company is converting its assets into new income. Which companies or industries are most efficient based on  their ROA (name top 10)?**
```
SELECT 
    dc.CompanyName,
    dc.Industry,
    (CAST(ff.Profit AS Float64) / NULLIF(CAST(ff.Assets AS Float64), 0)) AS roa
FROM gold.DimCompany dc
JOIN gold.FactFinancials ff ON dc.CompanyKey = ff.CompanyKey
WHERE ff.Assets > 0
ORDER BY roa DESC
LIMIT 10;
```
```
    ┌─CompanyName──────────────┬─Industry────────────────────────┬────────────────roa─┐
 1. │ Inhibrx Biosciences      │ Drugs & Biotechnology           │  9.337016574585636 │
 2. │ RattanIndia Power        │ Utilities                       │  1.131578947368421 │
 3. │ EXOR                     │ Diversified Financials          │ 0.7071651090342679 │
 4. │ NVIDIA                   │ Semiconductors                  │ 0.6530465949820788 │
 5. │ VeriSign                 │ IT Software & Services          │ 0.5448275862068965 │
 6. │ Americanas               │ Retailing                       │ 0.5441696113074205 │
 7. │ Savola Group             │ Food, Drink & Tobacco           │ 0.5325131810193322 │
 8. │ Monolithic Power Systems │ Semiconductors                  │  0.494475138121547 │
 9. │ Oi                       │ Telecommunications Services     │ 0.4708994708994709 │
10. │ Ubiquiti                 │ Technology Hardware & Equipment │  0.376271186440678 │
    └──────────────────────────┴─────────────────────────────────┴────────────────────┘
```
**4. Is the company over or undervalued compared to its peers? List for each industry the average P/E using only Forbes 2000 data and select the top company for each sector based on P/E including the delta with the average of that industry.**
```
WITH IndustryPE AS (
    SELECT 
        MAX(dd.TradingDate) as TradingDate,
        dc.Industry,
        AVG(fs.ClosePrice / NULLIF(ff.Profit, 0)) AS avg_pe
    FROM gold.FactStock fs
    JOIN gold.DimTicker dt ON fs.TickerKey = dt.TickerKey
    JOIN bronze.companies_raw cr ON dt.TickerSymbol = cr.ticker
    JOIN gold.DimCompany dc ON CAST(xxHash64(concat(cr.company, cr.headquarters)) AS UInt32) = dc.CompanyKey
    JOIN gold.FactFinancials ff ON dc.CompanyKey = ff.CompanyKey
    JOIN gold.FactForbesRank fr ON dc.CompanyKey = fr.CompanyKey
    JOIN gold.DimDate dd ON fs.DateKey = dd.DateKey
    WHERE fr.ForbesRank <= 2000 AND ff.Profit > 0 AND dd.TradingDate<=now()
    GROUP BY dc.Industry, dd.TradingDate
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
    ip.TradingDate,
    cp.Industry,
    cp.CompanyName,
    cp.company_pe,
    ip.avg_pe,
    cp.company_pe - ip.avg_pe AS delta
FROM CompanyPE cp
JOIN IndustryPE ip ON cp.Industry = ip.Industry
ORDER BY cp.Industry, delta DESC;
```
```
    ┌─TradingDate─┬─Industry────────────┬─CompanyName───────────────────┬───────────company_pe─┬─────────────avg_pe─┬─────────────────delta─┐
 1. │  2025-11-02 │ Aerospace & Defense │ Mitsubishi Heavy Industries   │    2.685207100591716 │ 0.5881085171045781 │    2.0970985834871376 │
 2. │  2025-11-02 │ Aerospace & Defense │ Rheinmetall                   │                  2.2 │ 0.5881085171045781 │     1.611891482895422 │
 3. │  2025-11-02 │ Aerospace & Defense │ Axon Enterprise               │   1.9651063829787234 │ 0.5881085171045781 │    1.3769978658741453 │
 4. │  2025-11-02 │ Aerospace & Defense │ TransDigm Group               │   0.7964268292682928 │ 0.5881085171045781 │   0.20831831216371466 │
 5. │  2025-11-02 │ Aerospace & Defense │ Bharat Electronics            │   0.6877516778523489 │ 0.5881085171045781 │    0.0996431607477708 │
 6. │  2025-11-02 │ Aerospace & Defense │ Teledyne Technologies         │   0.6245476477683957 │ 0.5881085171045781 │   0.03643913066381754 │
 7. │  2025-11-02 │ Aerospace & Defense │ Huntington Ingalls Industries │   0.5801272727272727 │ 0.5881085171045781 │ -0.007981244377305408 │
 8. │  2025-11-02 │ Aerospace & Defense │ Heico                         │   0.5473192239858906 │ 0.5881085171045781 │  -0.04078929311868751 │
 9. │  2025-11-02 │ Aerospace & Defense │ Rolls-Royce Holdings          │   0.3627329192546584 │ 0.5881085171045781 │  -0.22537559784991973 │
10. │  2025-11-02 │ Aerospace & Defense │ Dassault Aviation             │  0.27207207207207207 │ 0.5881085171045781 │  -0.31603644503250605 │
11. │  2025-11-02 │ Aerospace & Defense │ Thales                        │  0.25341964285714286 │ 0.5881085171045781 │  -0.33468887424743526 │
12. │  2025-11-02 │ Aerospace & Defense │ L3Harris Technologies         │  0.18493749999999998 │ 0.5881085171045781 │  -0.40317101710457814 │
13. │  2025-11-02 │ Aerospace & Defense │ Howmet Aerospace              │   0.1739396551724138 │ 0.5881085171045781 │   -0.4141688619321643 │
14. │  2025-11-02 │ Aerospace & Defense │ Northrop Grumman              │   0.1559568733153639 │ 0.5881085171045781 │   -0.4321516437892142 │
15. │  2025-11-02 │ Aerospace & Defense │ Textron                       │   0.0956867469879518 │ 0.5881085171045781 │   -0.4924217701166263 │
16. │  2025-11-02 │ Aerospace & Defense │ AIRBUS                        │   0.0464410480349345 │ 0.5881085171045781 │   -0.5416674690696436 │
17. │  2025-11-02 │ Aerospace & Defense │ GE Aerospace                  │  0.04458393113342898 │ 0.5881085171045781 │   -0.5435245859711492 │
18. │  2025-11-02 │ Aerospace & Defense │ RTX                           │   0.0385695652173913 │ 0.5881085171045781 │   -0.5495389518871868 │
19. │  2025-11-02 │ Aerospace & Defense │ Leonardo                      │  0.03107758620689655 │ 0.5881085171045781 │   -0.5570309308976815 │
20. │  2025-11-02 │ Aerospace & Defense │ ST Engineering                │ 0.016266666666666665 │ 0.5881085171045781 │   -0.5718418504379115 │
    └─────────────┴─────────────────────┴───────────────────────────────┴──────────────────────┴────────────────────┴───────────────────────┘
```
**5. Which industries generate the highest total revenue among Forbes 2000 companies? Name top 20.**
```
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
```
```
    ┌─Industry─────────────────────────┬─total_revenue─┐
 1. │ Banking                          │       5965831 │
 2. │ Oil & Gas Operations             │       4553410 │
 3. │ Insurance                        │       3880450 │
 4. │ Retailing                        │       3438999 │
 5. │ Consumer Durables                │       3290229 │
 6. │ Construction                     │       2246645 │
 7. │ Technology Hardware & Equipment  │       1881899 │
 8. │ Food, Drink & Tobacco            │       1684668 │
 9. │ Transportation                   │       1681260 │
10. │ Materials                        │       1528460 │
11. │ Utilities                        │       1497108 │
12. │ Drugs & Biotechnology            │       1458760 │
13. │ IT Software & Services           │       1408683 │
14. │ Capital Goods                    │       1388130 │
15. │ Telecommunications Services      │       1222410 │
16. │ Diversified Financials           │       1207526 │
17. │ Health Care Equipment & Services │       1144640 │
18. │ Trading Companies                │       1103660 │
19. │ Business Services & Supplies     │       1065879 │
20. │ Food Markets                     │       1064370 │
    └──────────────────────────────────┴───────────────┘
```
**6. Which industry among Forbes 2000 has had the highest annual increase in stock market price based on average y-o-y growth of closing price?**

  The query is not tested as there is not enough data to run the query. We do not have data over multiple years as of today.

## Troubleshooting

If for some reason airflow-webserver doesn't come up when running docker, try starting airflow-init again and wait for webserver to come up (takes ~1 minute).


