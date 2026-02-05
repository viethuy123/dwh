{{ config(materialized='table') }}

SELECT *

from {{ ref('skill_members') }}