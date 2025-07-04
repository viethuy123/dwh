WITH monthly_counts AS (
  SELECT
    ts.date AS month_end,
    ts.year,
    ts.month_name,
    e.branch_id,
    e.department_id,
    e.user_level,
    COUNT(DISTINCT e.staff_code) AS employee_count
  FROM dwh_fdw.time_series ts
  LEFT JOIN dwh_fdw.users e
    ON e.probation_date <= ts.date
    AND (e.quit_date IS NULL OR e.quit_date > ts.date)
  WHERE ts.is_month_end = TRUE
    AND ts.date BETWEEN '2005-01-31' AND CURRENT_DATE
  GROUP BY ts.date, ts.year, ts.month_name, e.branch_id, e.department_id, e.user_level
)
SELECT
  month_end,
  year,
  month_name,
  employee_count,
  LAG(employee_count) OVER (ORDER BY month_end) AS previous_count,
  employee_count - LAG(employee_count) OVER (ORDER BY month_end) AS absolute_change,
  ROUND(
    CASE
      WHEN LAG(nullif(employee_count,0)) OVER (ORDER BY month_end) = 0 THEN NULL
      ELSE ((employee_count - LAG(employee_count) OVER (ORDER BY month_end))::FLOAT / LAG(employee_count) OVER (ORDER BY month_end)) * 100
    END, 2) AS percent_change
FROM monthly_counts
ORDER BY month_end;


select * from jira.dim_projects where project_value is not null;


WITH project_values AS (
    SELECT 
        project_id,
        sum(value + value1) as project_value
    FROM dwh_fdw.project_profit_loss
    GROUP BY project_id
)
SELECT 
    a.project_id,
    jira_project_id,
    project_name,
    project_jira_url,
    project_lead,
    location,
    scope,
    type,
    point_css,
    summary,
    size,
    period,
    team_size,
    start_date,
    end_date,
    b.project_value
FROM dwh_fdw.projects a
LEFT JOIN project_values b
ON a.project_id = b.project_id


SELECT rolname FROM pg_roles WHERE rolname = 'dev_slave';
SELECT * FROM pg_publication;

SELECT * FROM pg_stat_replication;

create table test_table(id int, name varchar(255));

SELECT * FROM pg_replication_slots WHERE slot_name = 'replication_slot';

select * from hr.fct_employee_count where freq_code = 'Y'
    SELECT
      report_date,
      sum(employee_count) AS total_employees
    FROM
      hr.fct_employee_count
    WHERE
      freq_code = 'M'
    GROUP BY
      report_date


  SELECT
    ts.date AS report_date,
    -- e.branch_id,
    -- e.department_id,
    -- e.user_level,
    COUNT(DISTINCT e.staff_code) AS employee_count
  FROM dwh_fdw.time_series ts
  LEFT JOIN dwh_fdw.users e
    ON e.probation_date <= ts.date
    AND (e.quit_date IS NULL OR e.quit_date > ts.date or (e.probation_date <= e.quit_date and  e.quit_date <= ts.date))
  WHERE ts.is_month_end = TRUE
    AND ts.month = 12
    AND ts.date BETWEEN '2005-01-01' AND CURRENT_DATE
  GROUP BY ts.date
  

  select count(*) from dwh_fdw.users;


  select distinct user_level from users;


WITH yearly_absence as (
select 
  user_id, 
  sum(absent_day), 
  to_char(end_date,'YYYY-MM') as year_month 
from create_fdw.stg_staff_attendances 
group by to_char(end_date,'YYYY-MM'), user_id)

select a.*,b.user_id from 


select 
  user_id, 
  sum(absent_day), 
  extract(year from end_date) as year_month 
from staff_attendances 
group by extract(year from end_date), user_id

select * from create_fdw.stg_users a
where exists (select 1 from create_fdw.stg_staff_attendances b where b."userObjId" = a._id)


select * from hr.fct_hrm_employees where total_absence is not null;


select distinct emp_status from hr.fct_hrm_employees;

select count(DISTINCT staff_code) from users where  user_status <> 'Inactivity' and probation_date is null  and extract (year from probation_date) <= 2025

select * from users where probation_date is null

select min(emp_salary), max(emp_salary) from hr.fct_hrm_employees


select distinct issue_priority from jira.fct_issues


select distinct resolution from jira.fct_issues

select distinct status from jira.fct_issues



SELECT
  jira.dim_time_series.date,
  avg(
    extract(
      epoch
      FROM
        (i.resolution_date - i.created_time)
    )
  ) / 3600 AS avg_hour_resulotion
FROM
  jira.fct_issues i
  RIGHT JOIN jira.dim_time_series ON i.created_time <= jira.dim_time_series.date + INTERVAL '6 days'
  AND i.created_time >= jira.dim_time_series.date
  AND i.resolution_date > jira.dim_time_series.date
  AND i.resolution_date IS NOT NULL
GROUP BY
  jira.dim_time_series.date


select *  from jira.fct_issues where type = 'Bug';

select distinct status from jira.fct_issues;

SELECT
  p.project_name,
  p.project_lead,
  p.location,
  p.scope,
  p.size,
  COUNT(DISTINCT pm.user_id) AS team_size,
  (
    COUNT(*) FILTER (
      WHERE
        i.resolution_date <= i.due_date
    )
  )::float / COUNT(*)::float AS sla_compliance_percent
FROM
  jira.dim_projects p
  JOIN jira.fct_project_members pm ON p.project_id = pm.project_id
  JOIN jira.fct_issues i ON p.jira_project_id = i.jira_project_id
WHERE
  i.resolution_date IS NOT NULL
  AND i.due_date IS NOT NULL
GROUP BY
  p.project_name,
  p.project_lead,
  p.location,
  p.scope,
  p.size;


select sum(time_worked),
  jira.dim_members.member_name,
  jira.dim_members.member_email,
  jira.dim_members.staff_code,
  jira.dim_members.department_name,
  jira.dim_members.position_name,
  jira.dim_members.user_level
from jira.fct_worklog w
join jira.dim_members ON jira.dim_members.member_email = w.worklog_author
where worklog_author = 'hungnl@runsystem.net' and jira_project_id = '10444'
group by jira.dim_members.member_name,
  jira.dim_members.member_email,
  jira.dim_members.staff_code,
  jira.dim_members.department_name,
  jira.dim_members.position_name,
  jira.dim_members.user_level



WITH
  _projects AS (
    SELECT
      project_id,
      project_name,
      EXTRACT(DAY FROM (end_date - start_date)) AS project_days,
      jira_project_id
    FROM
      jira.dim_projects
    WHERE
      jira_project_id = '10444'
  ),
  _worklog_summary AS (
    SELECT
      w.worklog_author,
      w.jira_project_id,
      SUM(w.time_worked) AS total_hours_worked
    FROM
      jira.fct_worklog w
    WHERE
      w.jira_project_id = '10444'
    GROUP BY
      w.worklog_author,
      w.jira_project_id
  ),
  _issue_summary AS (
    SELECT
      i.issue_assignee,
      i.jira_project_id,
      COUNT(DISTINCT i.issue_id) FILTER (
        WHERE i.status = 'Closed'
      ) AS issues_resolved
    FROM
      jira.fct_issues i
    WHERE
      i.jira_project_id = '10444'
    GROUP BY
      i.issue_assignee,
      i.jira_project_id
  )
SELECT
  p.project_name,
  m.member_name,
  m.member_email,
  m.staff_code,
  m.department_name,
  m.position_name,
  m.user_level,
  COALESCE(ws.total_hours_worked, 0) AS total_hours_worked,
  COALESCE(iss.issues_resolved, 0) AS issues_resolved
FROM
  _projects p
  JOIN jira.fct_project_members pm ON pm.project_id = p.project_id
  JOIN jira.dim_members m ON pm.user_id = m.member_id
  LEFT JOIN _worklog_summary ws ON m.member_email = ws.worklog_author
    AND ws.jira_project_id = p.jira_project_id
  LEFT JOIN _issue_summary iss ON m.member_email = iss.issue_assignee
    AND iss.jira_project_id = p.jira_project_id
ORDER BY
  p.project_name,
  m.member_name;



select a.* 
from src_create.stg_project_members a 
join src_create.stg_pods b 
on a."projectObjId" = b._id


select * from projects where start_date > current_date

select * from etl_job_logs where target_table = 'stg_pods'

select * from etl_job_logs where status = 'FAILURE' order by created_time desc


select * from jira.dim_members where member_id = '608285d10e83773bc64b264f'