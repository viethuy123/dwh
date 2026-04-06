{{ config(materialized='table') }}

select * from {{ ref('create_attendance_types') }}