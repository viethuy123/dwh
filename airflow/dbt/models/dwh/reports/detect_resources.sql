{{ config(
    materialized="table",
    on_configuration_change="apply",
      indexes=[
      {
          "columns": ["member_email_full", "month_year", "skill_name"],
          "unique": true,
          "type": "btree",
      }
      ,
      {
          "columns": ["month_year"],
          "unique": false,
          "type": "btree",
      },
      
      {
          "columns": ["branch_code", "division_name", "month_year"],
          "unique": false,
          "type": "btree",
      },
      
      {
          "columns": ["member_name", "skill_name", "free_efforts"],
          "unique": false,
          "type": "btree",
      },
      
      {
          "columns": ["skill_name", "skill_level", "month_year"],
          "unique": false,
          "type": "btree",
      }
  ]
) }}

WITH

  dim_members as (
    select
      m.*,
      coalesce(m.official_date, m.joining_date, m.end_date)::DATE as create_date_used,
      m.member_code as staff_code,
      m.member_level as user_level,
      m.member_status_root as user_status,
      coalesce(m.end_date, '2999-12-31'::DATE) as end_date_used
    from {{ ref('dim_odoo_members') }} m

  ),

  _time_series AS (
    SELECT
      DATE_TRUNC('month',
        generate_series(
          DATE_TRUNC('month', DATE_TRUNC('year', CURRENT_DATE) - INTERVAL '3 years') + INTERVAL '11 months',
          DATE_TRUNC('month', CURRENT_DATE) + INTERVAL '3 months',
          INTERVAL '1 month'
        )
      )::DATE AS month_year
  ),

  all_member_future_months AS (
    SELECT
      m.member_email,
      ts.month_year
    FROM
      dim_members m
      CROSS JOIN _time_series ts
    WHERE
      ts.month_year >= DATE_TRUNC('month', m.create_date_used) 
      AND ts.month_year <= DATE_TRUNC('month', m.end_date_used)
  ),

  _jira_efforts AS (
    SELECT
      w.worklog_author as member_email,
      DATE_TRUNC('month', start_time)::DATE AS month_year,
      (sum(time_worked) / 3600) / 160 AS actual_efforts,
      avg((sum(time_worked) / 3600) / 160) OVER (
        PARTITION BY w.worklog_author
        ORDER BY DATE_TRUNC('month', start_time)
        ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
      ) AS ma4
    FROM
      {{ ref('fct_worklogs') }} w
    GROUP BY
      w.worklog_author,
      DATE_TRUNC('month', start_time)
  ),

  _pod_efforts_raw AS (
    SELECT
      member_id,
      CASE 
        WHEN month_year ~ '^\d{4}-\d{2}$' THEN 
          (month_year || '-01')::DATE
        ELSE 
          month_year::DATE
      END AS month_year_date,
      SUM(CASE WHEN effort != 0 THEN effort ELSE NULL END) AS pod_efforts
    FROM {{ ref('fct_pod_member_efforts') }}
  GROUP BY
      member_id,
      month_year_date
  ),

  _pod_efforts AS (
    SELECT
      m.member_email,
      pme.month_year_date as month_year,
      SUM(pme.pod_efforts) AS pod_efforts
    FROM
      _pod_efforts_raw pme
      JOIN dim_members m 
      ON m.member_id = pme.member_id
      and pme.month_year_date >= m.create_date_used
      and pme.month_year_date <= m.end_date_used
    GROUP BY
      m.member_email,
      pme.month_year_date
  ),



  _efforts AS (
    SELECT
      COALESCE(ts.member_email,je.member_email, pe.member_email) as member_email_full,
      COALESCE(je.member_email, pe.member_email) AS member_email,
      COALESCE(ts.month_year, je.month_year, pe.month_year) AS month_year,
      je.actual_efforts,
      je.ma4,
      pe.pod_efforts
    FROM 
      all_member_future_months ts
      FULL OUTER JOIN _pod_efforts pe 
        ON pe.month_year = ts.month_year AND pe.member_email = ts.member_email
      FULL OUTER JOIN _jira_efforts je 
        ON je.month_year = COALESCE(ts.month_year, pe.month_year) 
       AND je.member_email = COALESCE(ts.member_email, pe.member_email)
  ),

  _efforts_with_actual AS (
    SELECT
      member_email_full,
      member_email,
      month_year,
      actual_efforts,
      ma4,
      pod_efforts,
      COALESCE(actual_efforts, pod_efforts) AS actual_pod_efforts
    FROM _efforts
  ),

  -- _efforts_with_past_avg AS (
  --   SELECT
  --     *,
  --     AVG(actual_pod_efforts) OVER (
  --       PARTITION BY member_email_full
  --       ORDER BY month_year
  --       ROWS BETWEEN 4 PRECEDING AND 1 PRECEDING
  --     ) AS avg_actual_last4
  --   FROM _efforts_with_actual
  -- ),

_efforts_with_past_avg AS (
    SELECT
      *,
      -- Chỉ tính trung bình nếu có ít nhất 1 dòng có dữ liệu thực tế (actual/pod) trong 4 tháng trước
      CASE 
        WHEN COUNT(actual_pod_efforts) OVER (
          PARTITION BY member_email_full 
          ORDER BY month_year 
          ROWS BETWEEN 4 PRECEDING AND 1 PRECEDING
        ) > 0 THEN
          AVG(actual_pod_efforts) OVER (
            PARTITION BY member_email_full
            ORDER BY month_year
            ROWS BETWEEN 4 PRECEDING AND 1 PRECEDING
          ) 
        ELSE NULL 
      END AS avg_actual_last4
    FROM _efforts_with_actual
  ),

  _predicting_pass1 AS (
    SELECT
      member_email_full,
      member_email,
      month_year,
      actual_efforts,
      actual_pod_efforts,
      pod_efforts,
      ma4,
      avg_actual_last4,
      CASE
        -- Tháng hiện tại hoặc quá khứ: Nếu 4 tháng trước toàn NULL thì kết quả là NULL
        WHEN month_year <= DATE_TRUNC('month', CURRENT_DATE) THEN
          avg_actual_last4
        ELSE
          NULL
      END AS new_predicting_efforts
    FROM _efforts_with_past_avg
  ),

  _predicting_pass2 AS (
    SELECT
      member_email_full,
      member_email,
      month_year,
      actual_efforts,
      actual_pod_efforts,
      pod_efforts,
      ma4,
      COALESCE(
        new_predicting_efforts,
        CASE 
          WHEN month_year = DATE_TRUNC('month', CURRENT_DATE) + INTERVAL '1 month' 
          -- Kiểm tra 4 tháng trước có dữ liệu thực tế hay không
          AND COUNT(actual_pod_efforts) OVER (PARTITION BY member_email_full ORDER BY month_year ROWS BETWEEN 4 PRECEDING AND 1 PRECEDING) > 0 
          THEN
            AVG(COALESCE(actual_pod_efforts, new_predicting_efforts)) OVER (
              PARTITION BY member_email_full
              ORDER BY month_year
              ROWS BETWEEN 4 PRECEDING AND 1 PRECEDING
            )
        END
      ) AS new_predicting_efforts
    FROM _predicting_pass1
  ),

  _predicting_pass3 AS (
    SELECT
      member_email_full,
      member_email,
      month_year,
      actual_efforts,
      actual_pod_efforts,
      pod_efforts,
      ma4,
      COALESCE(
        new_predicting_efforts,
        CASE 
          WHEN month_year = DATE_TRUNC('month', CURRENT_DATE) + INTERVAL '2 months' 
          -- Tiếp tục kiểm tra 4 tháng trước (bao gồm cả các tháng đã dự báo ở Pass trước) có dữ liệu thực tế hay không
          AND COUNT(actual_pod_efforts) OVER (PARTITION BY member_email_full ORDER BY month_year ROWS BETWEEN 4 PRECEDING AND 1 PRECEDING) > 0 
          THEN
            AVG(COALESCE(actual_pod_efforts, new_predicting_efforts)) OVER (
              PARTITION BY member_email_full
              ORDER BY month_year
              ROWS BETWEEN 4 PRECEDING AND 1 PRECEDING
            )
        END
      ) AS new_predicting_efforts
    FROM _predicting_pass2
  ),

  _predicting_pass4 AS (
    SELECT
      member_email_full,
      member_email,
      month_year,
      actual_efforts,
      actual_pod_efforts,
      pod_efforts,
      ma4,
      COALESCE(
        new_predicting_efforts,
        CASE 
          WHEN month_year = DATE_TRUNC('month', CURRENT_DATE) + INTERVAL '3 months' 
          AND COUNT(actual_pod_efforts) OVER (PARTITION BY member_email_full ORDER BY month_year ROWS BETWEEN 4 PRECEDING AND 1 PRECEDING) > 0 
          THEN
            AVG(COALESCE(actual_pod_efforts, new_predicting_efforts)) OVER (
              PARTITION BY  member_email_full
              ORDER BY month_year
              ROWS BETWEEN 4 PRECEDING AND 1 PRECEDING
            )
        END
      ) AS new_predicting_efforts
    FROM _predicting_pass3
  ),

  _predicting_efforts AS (
    SELECT
      member_email_full,
      member_email,
      month_year,
      actual_efforts,
      actual_pod_efforts,
      pod_efforts,
      LAG(ma4) OVER (
        PARTITION BY member_email_full
        ORDER BY month_year
      ) AS predicting_efforts,
      new_predicting_efforts
    FROM _predicting_pass4
  ),

  _final AS (
    SELECT
    
      member_email_full,
      member_email,
      month_year,
      actual_efforts,
      actual_pod_efforts,
      new_predicting_efforts,
      predicting_efforts,
      pod_efforts,
      1 as normal_efforts
    FROM _predicting_efforts
  ),
all_data AS (
SELECT
  f.member_email_full,
  m.member_name,
  m.staff_code,
  m.branch_name,
  m.branch_code,
  m.division_name,
  m.position_name,
  m.user_level,
  m.user_status,
  f.member_email,
  COALESCE(f.month_year, DATE_TRUNC('month', NOW())::DATE) as month_year,
  f.actual_efforts,
  f.pod_efforts,
  f.new_predicting_efforts as predicting_efforts,
  f.normal_efforts,
  CASE 
    WHEN COALESCE(f.month_year, DATE_TRUNC('month', NOW())::DATE) < DATE_TRUNC('month', NOW())::DATE 
    THEN f.normal_efforts - COALESCE(f.actual_efforts, f.pod_efforts, 0)
    ELSE f.normal_efforts - COALESCE(f.actual_efforts, f.pod_efforts, f.new_predicting_efforts, 0)
  END AS free_efforts
  FROM
  dim_members m
  LEFT JOIN  _final as f
  ON m.member_email = f.member_email_full
  AND f.month_year >= m.create_date_used
  AND f.month_year <= m.end_date_used
  WHERE COALESCE(f.month_year, DATE_TRUNC('month', NOW())::DATE) <= DATE_TRUNC('month', NOW()) + INTERVAL '3 months'
  and m.member_name is not null
  and m.member_name not in ('null', 'Admin')
  and f.member_email_full is not null
  -- and f.member_email_full like '%@runsystem%'
  -- AND branch_code != 'CNTO'
  -- AND (department_name != 'Nikko' OR department_name IS NULL)
  AND staff_code is not null
),
cacul_effort_type as
(
  SELECT
  * , 
  CASE 
    WHEN free_efforts <0 THEN 'Overloaded'
		WHEN free_efforts >= 0 and free_efforts <= 0.2  THEN 'Normal'
		WHEN free_efforts > 0.6 THEN 'Free'
		WHEN free_efforts > 0.2 THEN ' Underloaded'
  END AS efforts_status,
  CASE
    WHEN predicting_efforts IS NULL THEN 'No'
    ELSE 'Yes'
  END AS has_history_efforts_4_months 

  FROM
    all_data
),
skill_members as (
  select 
    a.staff_code, 
    a.skill_name, 
    a.skill_parent, 
    a.skill_level
  from (
    select 
      staff_code, 
      skill_name, 
      skill_total as skill_parent, 
      level_sub_skill as skill_level,
      row_number() over (
        partition by staff_code, skill_name 
        order by (select null)
      ) as rn
    from {{ ref('dim_skill_members') }}
  ) as a
  where rn = 1
),

skill_member_with_weight as (
  SELECT staff_code , skill_name, skill_parent, skill_level,
  COUNT(*) OVER(PARTITION by staff_code)
  
  as weight_factor
  from skill_members

),
get_etl as (
  select 
    MAX(etl_datetime) as etl_datetime
  FROM
      {{ ref('fct_worklogs') }}
)

select r.*, s.skill_name, s.skill_parent, s.skill_level,
case 
        when max(r.member_email) over(partition by r.member_email_full) is not null 
        then '1' 
        else '0'  
    end as log_work_status,
  r.free_efforts/COALESCE(s.weight_factor, 1) as free_effort_unique,
  g.etl_datetime
 from
cacul_effort_type as r
left join
skill_member_with_weight as s
on r.staff_code = s.staff_code
cross join
get_etl as g

