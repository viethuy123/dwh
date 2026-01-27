{{ config(materialized='table') }}

SELECT staff_code , skill_name from {{ ref('skill_members') }}