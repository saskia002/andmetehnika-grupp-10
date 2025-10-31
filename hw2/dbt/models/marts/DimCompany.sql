
SELECT
    CompanyKey,  -- Primary Key - should not change whatever happens to company
    CompanyName,  
    Headquarters,
    Industry,
    Sector,
    ValidFrom,   -- NEW FIELD compared to PR1: as we changes DimCompany to SDC2 then I add ValidFrom and ValidTo dates
    ValidTo      -- NEW FIELD compares to PR1
FROM {{ ref('stg_DimCompany') }}