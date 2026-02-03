{{ config(materialized='table') }}

WITH
jisseki_pro as (
    select 
    id::TEXT as project_id,
    'jisseki' as project_source,
    point_customer,
    amount,
    man_month,
    num_month,
    null::TEXT as project_size,
    etl_datetime

    from {{ ref('jisseki_project') }}
),
pod_pro as (
    select 
    pod_id::TEXT as project_id,
    'pod' as project_source,
    0 as point_customer,
    NULL as amount,
    0 as man_month,
    0 as num_month,
    project_size,
    etl_datetime

    
    from {{ ref('pods') }}
)

select * from jisseki_pro
union
select * from pod_pro
