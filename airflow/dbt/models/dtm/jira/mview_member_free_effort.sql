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
      TO_CHAR(
        generate_series(
          DATE_TRUNC(
            'month',
            DATE_TRUNC('year', CURRENT_DATE) - INTERVAL '5 years'
          ) + INTERVAL '11 months',
          DATE_TRUNC('month', DATE_TRUNC('year', CURRENT_DATE)) + INTERVAL '11 months',
          INTERVAL '1 month'
        ) + INTERVAL '1 month - 1 day',
        'YYYY-MM'
      ) AS month_year
  ),
  _jira_efforts AS (
    SELECT
      m.member_email,
      to_char(start_time, 'YYYY-MM') AS month_year,
      (sum(time_worked) / 3600) / 160 AS actual_efforts,
      avg((sum(time_worked) / 3600) / 160) OVER (
        ORDER BY
          to_char(start_time, 'YYYY-MM') ROWS BETWEEN 3 PRECEDING
          AND CURRENT ROW
      ) AS ma4,
      count(DISTINCT m.member_id) AS normal_efforts
    FROM
      {{ ref('fct_worklog') }} w
      JOIN {{ref('dim_members')}} m ON m.member_email = w.worklog_author
    GROUP BY
      m.member_email,
      to_char(start_time, 'YYYY-MM')
  ),
  _pod_efforts AS (
    SELECT
      m.member_email,
      month_year,
      sum(effort) AS pod_efforts,
      count(DISTINCT m.member_id) AS normal_efforts
    FROM
      {{ref('fct_pod_member_efforts')}} pme
      JOIN {{ref('dim_members')}} m ON m.member_id = pme.user_id
    GROUP BY
      m.member_email,
      month_year
  ),
  _efforts AS (
    SELECT
      COALESCE(je.member_email, pe.member_email) AS member_email,
      COALESCE(je.month_year, pe.month_year, ts.month_year) AS month_year,
      COALESCE(je.normal_efforts, pe.normal_efforts) AS normal_efforts,
      actual_efforts,
      ma4,
      pod_efforts
    FROM
      _time_series ts
      FULL OUTER JOIN _pod_efforts pe ON pe.month_year = ts.month_year
      FULL OUTER JOIN _jira_efforts je ON je.month_year = ts.month_year
      AND je.member_email = pe.member_email
  ),
  _predicting_efforts AS (
    SELECT
      member_email,
      month_year,
      actual_efforts,
      lag(ma4) OVER (
        PARTITION BY
          member_email
        ORDER BY
          month_year
      ) AS predicting_efforts,
      pod_efforts,
      normal_efforts
    FROM
      _efforts
  ),
  _final AS (
    SELECT
      member_email,
      month_year,
      actual_efforts,
      first_value(predicting_efforts) OVER (
        PARTITION BY
          x,
          member_email
        ORDER BY
          month_year,
          x NULLS FIRST
      ) AS predicting_efforts,
      pod_efforts,
      normal_efforts
    FROM
      (
        SELECT
          *,
          SUM(
            CASE
              WHEN predicting_efforts IS NOT NULL THEN 1
            END
          ) OVER (
            PARTITION BY
              member_email
            ORDER BY
              month_year
          ) AS x
        FROM
          _predicting_efforts
      ) t
  )
SELECT
  *,
  normal_efforts - COALESCE(actual_efforts, pod_efforts, predicting_efforts) AS free_efforts
FROM
  _final
WHERE member_email is not null