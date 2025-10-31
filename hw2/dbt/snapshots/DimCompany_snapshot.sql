{% snapshot DimCompany_snapshot %}
{{ config(
    unique_key='CompanyKey',
    strategy='check',
    check_cols=['CompanyName', 'Industry','Headquarters','Sector']
) }}

SELECT
    CompanyName,
    Headquarters,
    Industry,
    Sector
FROM {{ ref('DimCompany') }}
{% endsnapshot %}