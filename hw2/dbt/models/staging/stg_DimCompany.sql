
{{ config(
    materialized='table',
    schema='silver'
) }}

SELECT
<<<<<<< HEAD
    abs(mod(cast(hash(company) as bigint), 1000000000)) as CompanyKey,  -- Primary Key - unfortunately we are not able to load it from source and thus need to generate from name. should not change whatever happens to company
    company as CompanyName,  
    headquarters as Headquarters,
    industry as Industry,
    Sector --TOBEDELETED IF not existing
FROM {{ source('bronze', 'companies_raw') }};
=======
    CAST(xxHash64(concat(company,headquarters)) AS UInt32) AS CompanyKey,  -- Primary Key
    CAST(company AS String) AS CompanyName,
    CAST(headquarters AS String) AS Headquarters,
    CAST(industry AS String) AS Industry,
    CAST('sector' AS String) AS Sector,  -- optional, can drop later if not needed
    CAST(_ingested_at AS DateTime) AS _load_datetime
FROM {{ source('bronze', 'companies_raw') }}
>>>>>>> dbt
