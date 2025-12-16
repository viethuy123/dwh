{{ config(
    materialized="materialized_view",
    on_configuration_change="apply",
      indexes=[
      {
          "columns": ["member_email", "month_year"],
          "unique": true,
          "type": "btree",
      }
  ]
) }}

WITH

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
      {{ ref('dim_memberss') }} m
      CROSS JOIN _time_series ts
    WHERE
      ts.month_year >= DATE_TRUNC('month', m.create_date_used) 
      AND ts.month_year <= DATE_TRUNC('month', m.end_date)
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
      JOIN {{ ref('dim_memberss') }} m ON m.member_email = w.worklog_author
    GROUP BY
      w.worklog_author,
      DATE_TRUNC('month', start_time)
  ),

  _pod_efforts_raw AS (
    SELECT
      m.member_email,
      pme.month_year as month_year_text,
      SUM(CASE WHEN effort != 0 THEN effort ELSE NULL END) AS pod_efforts
    FROM
      {{ ref('fct_pod_member_effortss') }} pme
      JOIN {{ref('dim_memberss') }} m ON m.member_id = pme.member_id
    GROUP BY
      m.member_email,
      pme.month_year
  ),

  _pod_efforts AS (
    SELECT
      member_email,
      CASE 
        WHEN month_year_text ~ '^\d{4}-\d{2}$' THEN 
          (month_year_text || '-01')::DATE
        ELSE 
          month_year_text::DATE
      END AS month_year,
      pod_efforts
    FROM _pod_efforts_raw
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

  _efforts_with_past_avg AS (
    SELECT
      *,
      AVG(actual_pod_efforts) OVER (
        PARTITION BY member_email_full
        ORDER BY month_year
        ROWS BETWEEN 4 PRECEDING AND 1 PRECEDING
      ) AS avg_actual_last4
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
          WHEN month_year = DATE_TRUNC('month', CURRENT_DATE) + INTERVAL '1 month' THEN
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
          WHEN month_year = DATE_TRUNC('month', CURRENT_DATE) + INTERVAL '2 months' THEN
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
          WHEN month_year = DATE_TRUNC('month', CURRENT_DATE) + INTERVAL '3 months' THEN
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
  m.department_name,
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
  {{ ref('dim_memberss') }} m
  LEFT JOIN  _final as f
  ON m.member_email = f.member_email_full
  AND f.month_year >= DATE_TRUNC('month', m.create_date_used) 
  AND f.month_year <= DATE_TRUNC('month', m.end_date)
  WHERE COALESCE(f.month_year, DATE_TRUNC('month', NOW())::DATE) <= DATE_TRUNC('month', NOW()) + INTERVAL '3 months'
  and m.member_name is not null
  and m.member_name not in ('null', 'Admin')
  and f.member_email_full is not null
  and f.member_email_full like '%@runsystem%'
  AND branch_code != 'CNTO'
)
SELECT
  * , 
  CASE 
    WHEN free_efforts <0 THEN 'Overloaded'
		WHEN free_efforts >= 0 and free_efforts <= 0.2  THEN 'Normal'
		WHEN free_efforts > 0.6 THEN 'Free'
		WHEN free_efforts > 0.2 THEN 'Unoverload'
  END AS efforts_status,
  CASE
    WHEN predicting_efforts IS NULL THEN 'No'
    ELSE 'Yes'
  END AS has_history_efforts_4_months

FROM
  all_data