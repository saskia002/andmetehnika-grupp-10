-- Grant SELECT on full views to role _full
GRANT USAGE ON DATABASE gold_full_views TO analyst_full;
GRANT SELECT ON gold_full_views.v_DimCompany  TO analyst_full;
GRANT SHOW ON DATABASE gold TO analyst_full;

-- Grant SELECT on masked views to role _limited
GRANT USAGE ON DATABASE gold_limited_views TO analyst_limited;
GRANT SELECT ON gold_limited_views.*  TO analyst_limited;
GRANT SHOW ON DATABASE gold TO analyst_limited;