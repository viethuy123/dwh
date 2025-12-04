{% snapshot customer_snapshot %}

{{
    config(
        unique_key='customer_id',
        strategy='check',
        check_cols=['company_name', 'sale_person', 'status'], 
        invalidate_hard_deletes=True,
        tags = ['scd2','customer']
    )
}}

SELECT 
    customer_id, 
    company_name, 
    customer_name,
    sale_person,
    status,
    type,
    country_id,
    updated_at
FROM {{ source('dwh', 'jisseki_customers') }}

{% endsnapshot %}