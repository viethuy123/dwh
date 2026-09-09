"""Report-centric dbt orchestration with Cosmos.

Dùng path: selector để chạy TẤT CẢ reports trong 1 DbtTaskGroup.
Cosmos tự parse ref() và sắp xếp thứ tự, KHÔNG chạy lại upstream dim/fct
(vì chúng đã chạy ở DAG riêng trước đó).
"""
from __future__ import annotations

from datetime import timedelta

from airflow.datasets import Dataset
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import DAG

from config import DEFAULT_ARGS, DEFAULT_CHECK_DAG
from factories.cosmos_factory import build_layer_task_group

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

    # 1 DbtTaskGroup cho cả folder reports — không duplicate upstream
    reports_group = build_layer_task_group("reports", "models/dwh/reports")

    end = EmptyOperator(
        task_id="end",
        outlets=[Dataset("all_reports_completed")],
        trigger_rule=DEFAULT_CHECK_DAG["trigger_rule"],
    )

    start >> reports_group >> end