{{ config(materialized='table') }}

SELECT staff_code , skill_name, etl_datetime

from {{ ref('skill_members') }}