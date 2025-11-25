-- Grant SELECT on full views to role _full
GRANT SELECT ON gold.v_FactFinancials  TO analyst_full;
GRANT SELECT ON gold.v_FactStock  TO analyst_full;
GRANT SELECT ON gold.v_FactForbesRank  TO analyst_full;
GRANT SELECT ON gold.v_DimCompany TO analyst_full;
GRANT SELECT ON gold.v_DimDate  TO analyst_full;
GRANT SELECT ON gold.v_DimTicker  TO analyst_full;

-- Grant SELECT on masked views to role _limited
GRANT SELECT ON gold.v_limited_FactFinancials  TO analyst_limited;
GRANT SELECT ON gold.v_limited_FactStock  TO analyst_limited;
GRANT SELECT ON gold.v_limited_FactForbesRank  TO analyst_limited;
GRANT SELECT ON gold.v_limited_DimCompany TO analyst_limited;
GRANT SELECT ON gold.v_limited_DimDate  TO analyst_limited;
GRANT SELECT ON gold.v_limited_DimTicker  TO analyst_limited;