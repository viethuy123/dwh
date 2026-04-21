-- models/marts/hr/fct_member_skill.sql
{{ config(materialized='table') }}

select * from {{ ref('member_attendance_daily') }}