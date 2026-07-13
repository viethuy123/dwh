{{ config(materialized='table') }}

select
    date_trunc('month',ot.from_date)::date as report_month,
    sum(ot.absent_day*ot.absent_hour) as ot_hour,
    o.member_code


from {{ ref('create_staff_overtime_details') }} ot
    join {{ ref('bridge_member_create_with_odoo')}} o
    on ot.member_id = o.member_id
    where ot.status_approval = 'APPROVED'
    and ot.is_deleted IS FALSE
group by
    date_trunc('month',ot.from_date),
    o.member_code
    