{% snapshot DimCompany_snapshot %}
{{ config(
    target_schema='forbes_2000',
    unique_key='CompanyKey',
    strategy='check',
    check_cols=['CompanyName', 'Headquarters', 'Industry', 'Sector'] -- unfortuantely we had to generate companykey from name and thus we cannot really catch name changes
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