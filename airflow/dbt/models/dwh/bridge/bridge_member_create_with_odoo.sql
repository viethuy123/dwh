{{ config(materialized='table') }}

Select 
    user_id as member_id,
    staff_code as member_code

from
    {{ ref('users') }}
group by 1,2
