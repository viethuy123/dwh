{{ config(materialized='table') }}
with end_date as (
    select 
        *,
        CASE 
            WHEN LEAD(created_at, 1, null) OVER (
                PARTITION BY email_company
                ORDER BY created_at ASC 
            ) = TIMESTAMP '2999-12-31' 
            THEN DATE '2999-12-31'
            ELSE (DATE_TRUNC('month', 
                LEAD(created_at, 1, null) OVER (
                    PARTITION BY email_company
                    ORDER BY created_at ASC 
                )
            ) - INTERVAL '1 day')::DATE
        END AS end_date_1
    from {{ ref('users_infos') }}
    where staff_code is not null
),
add_quite_date as (
    select 
        *,
        COALESCE(
            quit_date, 
            MAX(quit_date) OVER (PARTITION BY staff_code)
        ) as quit_date_use,
        COALESCE(
            official_date, 
            MAX(official_date) OVER (PARTITION BY staff_code)
        ) as official_date_use,
        COALESCE(
            probation_date, 
            MAX(probation_date) OVER (PARTITION BY staff_code)
        ) as probation_date_use,
        COALESCE(
            intern_date, 
            MAX(intern_date) OVER (PARTITION BY staff_code)
        ) as intern_date_use,
        ROW_NUMBER() OVER (
            PARTITION BY staff_code
            ORDER BY created_at DESC
        ) as row_num
    from end_date
),
get_latest_record as (
    select *,
    coalesce(end_date_1,quit_date_use) as end_date
    from add_quite_date
    where row_num = 1
)
-- get_data_users as (
--     select *
--     FROM {{ ref('users') }} u
-- ),
-- get_final_data as (
--     select u.* , ui.
--     from get_data_users u
--     join get_latest_record ui on u.user_id = ui.user_id
-- )

SELECT
    * 
from get_latest_record

