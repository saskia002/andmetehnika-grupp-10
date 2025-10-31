
{{ config(
    materialized='table',
    schema='Forbes_2000'
) }}

SELECT
    CompanyKey,  -- Primary Key - should not change whatever happens to company
    CompanyName,  
    Headquarters,
    Industry,
    Sector,
    dbt_valid_from AS ValidFrom,    -- NEW FIELD compared to PR1: as we changes DimCompany to SDC2 then I add ValidFrom and ValidTo dates
    dbt_valid_to   AS ValidTo       -- NEW FIELD compares to PR1
FROM {{ ref('DimCompany_snapshot') }}