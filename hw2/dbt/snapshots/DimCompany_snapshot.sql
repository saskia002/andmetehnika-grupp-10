{% snapshot DimCompany_snapshot %}
{{ config(
    target_schema='snapshots',
    unique_key='CompanyKey',
    strategy='check',
    check_cols=['CompanyName', 'Headquarters', 'Industry', 'Sector'] 
) }}

SELECT
    CompanyKey,
    CompanyName,
    Headquarters,
    Industry,
    Sector,
    _load_datetime
FROM {{ ref('stg_DimCompany') }}
{% endsnapshot %}