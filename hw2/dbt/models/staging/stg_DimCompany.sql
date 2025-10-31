

SELECT
    abs(mod(cast(hash(company) as bigint), 1000000000)) as CompanyKey,  -- Primary Key - unfortunately we are not able to load it from source and thus need to generate from name. should not change whatever happens to company
    company as CompanyName,  
    headquarters as Headquarters,
    industry as Industry,
    Sector --TOBEDELETED IF not existing
FROM {{ source('bronze', 'companies_raw') }};
