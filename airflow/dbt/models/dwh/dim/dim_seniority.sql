{{ config(materialized='table') }}

SELECT 1 AS seniority_group_id, '1.  < 2 tháng'      AS seniority_group, 1 AS sort_order
UNION ALL
SELECT 2, '2.  2 – < 6 tháng', 2
UNION ALL
SELECT 3, '3.  6 – < 12 tháng', 3
UNION ALL
SELECT 4, '4.  1 – < 2 năm',   4
UNION ALL
SELECT 5, '5.  2 – < 3 năm',   5
UNION ALL
SELECT 6, '6.  3 – < 6 năm',   6
UNION ALL
SELECT 7, '7.  >= 6 năm',      7
ORDER BY sort_order