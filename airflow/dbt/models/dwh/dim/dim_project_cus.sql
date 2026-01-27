{{ config(materialized='table') }}

WITH 
create_jira_cus as (
    select 
    customer_id::TEXT as customer_id,
    customer_name::TEXT as customer_name
    
    from {{ ref('create_project_customer') }}
),
jisseki_cus as (
    select 
    id::TEXT as customer_id,
    company_name::TEXT as customer_name
    from {{ ref('jisseki_customers') }}
)
select * from create_jira_cus
union
select * from jisseki_cus