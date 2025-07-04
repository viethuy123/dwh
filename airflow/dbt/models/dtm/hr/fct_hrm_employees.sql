{{ config(materialized='table') }}


WITH yearly_absence as (
    select 
        user_id as emp_id, 
        sum(absent_day) as total_absence, 
        to_char(end_date,'YYYY') as report_year
    FROM {{ source('dwh', 'staff_attendances') }}
    group by to_char(end_date,'YYYY'), user_id
)
SELECT
    ts.date as report_date,
    e.user_id as emp_id,
    e.user_name as emp_name,
    e.birthday as emp_dob,
    EXTRACT(YEAR FROM AGE(ts.date, e.birthday)) AS emp_age,
    e.gender as emp_gender,
    e.company_email as emp_email,
    e.staff_code,
    e.branch_id,
    e.department_id,
    e.position_id,
    e.user_level as emp_level,
    e.user_status as emp_status,
    e.probation_date,
    e.quit_date,
    ROUND(
        CAST(
            EXTRACT(EPOCH FROM AGE(ts.date, DATE_TRUNC('year', e.probation_date))) 
        / (365.25 * 24 * 60 * 60) AS NUMERIC
        ), 
        1
    ) AS tenure_years,
    y.total_absence,
    CASE 
        WHEN e.user_level = 'FRESHER' THEN 13*FLOOR(280 + (RANDOM() * (400 - 280 + 1)))::INTEGER
        WHEN e.user_level = 'JUNIOR' THEN 13*FLOOR(400 + (RANDOM() * (600 - 400 + 1)))::INTEGER
        WHEN e.user_level = 'MIDDLE' THEN 13*FLOOR(600 + (RANDOM() * (1000 - 600 + 1)))::INTEGER
        WHEN e.user_level = 'SENIOR' THEN 13*FLOOR(1000 + (RANDOM() * (1600 - 1000 + 1)))::INTEGER
        WHEN e.user_level = 'EXPERT' THEN 13*FLOOR(1600 + (RANDOM() * (2400 - 1600 + 1)))::INTEGER
        WHEN e.user_level = 'MANAGER' THEN 13*FLOOR(1200 + (RANDOM() * (2000 - 1200 + 1)))::INTEGER
        WHEN e.user_level = 'DIRECTOR' THEN 13*FLOOR(2000 + (RANDOM() * (3200 - 2000 + 1)))::INTEGER
        WHEN e.user_level = 'VISIONARY LEADER' THEN 13*FLOOR(3200 + (RANDOM() * (4800 - 3200 + 1)))::INTEGER
        ELSE null
    END AS emp_salary,
    FLOOR(RANDOM() * 10 + 1)::INTEGER AS satisfaction_score,
    ROUND((RANDOM() * 4 + 1)::NUMERIC, 2) AS engagement_score,
    (ARRAY['Exceeds', 'Fully Meets', 'PIP', 'Needs Improvement'])[FLOOR(RANDOM() * 4 + 1)::INTEGER] AS performance_score
FROM {{ source('dwh', 'time_series') }} ts
LEFT JOIN {{ source('dwh', 'users') }} e
ON e.probation_date <= ts.date
    AND (
        e.quit_date IS NULL OR e.quit_date >= DATE_TRUNC('month', ts.date) 
    )
LEFT JOIN yearly_absence y
ON to_char(ts.date,'YYYY') = y.report_year
    AND e.user_id = y.emp_id
WHERE ts.is_month_end = TRUE
    AND e.probation_date IS NOT NULL
    AND ts.month = 12
    AND ts.date BETWEEN DATE_TRUNC('year', CURRENT_DATE - INTERVAL '20 years') AND CURRENT_DATE
