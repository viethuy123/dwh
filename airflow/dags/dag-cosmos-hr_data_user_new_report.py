"""Report-centric dbt orchestration with Cosmos.

Các report chạy tuần tự để tránh conflict shared models (vd: user.sql).
Thêm report mới: cập nhật REPORTS trong config/cosmos_config.py.
"""
from __future__ import annotations

from datetime import timedelta

from airflow.datasets import Dataset
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import DAG

from config import DEFAULT_ARGS, DEFAULT_CHECK_DAG
from config.cosmos_config import REPORTS
from factories import build_report_task_group

dag = DAG(
    dag_id="dag_cosmos_reports_report_centric",
    default_args=DEFAULT_ARGS,
    schedule=[Dataset("staging_to_dwh_completed")],
    catchup=False,
    max_active_tasks=1,
    dagrun_timeout=timedelta(minutes=60),
    description="Cosmos dbt report-centric pipeline cho tất cả reports",
    tags=["dbt", "cosmos", "report-centric"],
)

with dag:
    start = EmptyOperator(task_id="start")

    end = EmptyOperator(
        task_id="end",
        outlets=[Dataset("all_reports_completed")],
        trigger_rule=DEFAULT_CHECK_DAG["trigger_rule"],
    )

    prev_task = start
    for report_name in REPORTS:
        report_task = build_report_task_group(report_name)
        prev_task >> report_task
        prev_task = report_task

    prev_task >> end