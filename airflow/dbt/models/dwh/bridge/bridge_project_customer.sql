
    {{ config(enabled=false) }}

        WITH
        create_jira_project as (
            select 
            cp.id::TEXT as project_id,
            cp.project_customer_code::TEXT as customer_id,
            etl_datetime
        from {{ ref('create_project') }} cp
        ),

        jisseki_project as (
            select 
            project_id::TEXT as project_id,
            customer_id::TEXT as customer_id,
            etl_datetime
        from {{ ref('jisseki_project_cus') }}
        ),
        pod_project as (
            select 
                pod_id::TEXT as project_id,
                customer_id::TEXT as customer_id,
                etl_datetime
            from {{ ref('pods') }}
        )

        select * from create_jira_project
        union
        select * from jisseki_project
        union
        select * from pod_project
    

