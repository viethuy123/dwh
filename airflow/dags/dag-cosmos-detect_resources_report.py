"""Report-centric dbt orchestration with Cosmos.

Sample DAG for one report: detect_resources.
Cosmos renders dbt models as Airflow tasks using the dbt ref() graph.
"""
from __future__ import annotations

from datetime import timedelta

from airflow.datasets import Dataset
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import DAG
from cosmos import DbtTaskGroup, ExecutionConfig, ProfileConfig, ProjectConfig, RenderConfig

from config import DEFAULT_ARGS, DEFAULT_CHECK_DAG


DBT_PROJECT_DIR = "/opt/airflow/dbt"
DBT_PROFILES_DIR = "/opt/airflow/dbt"
DBT_EXECUTABLE_PATH = "/home/airflow/.local/bin/dbt"

REPORT_NAME = "detect_resources"


profile_config = ProfileConfig(
    profile_name="dwh_project",
    target_name="dwh",
    profiles_yml_filepath=f"{DBT_PROFILES_DIR}/profiles.yml",
)

project_config = ProjectConfig(
    dbt_project_path=DBT_PROJECT_DIR,
    install_dbt_deps=True,
)

execution_config = ExecutionConfig(
    dbt_executable_path=DBT_EXECUTABLE_PATH,
)


dag = DAG(
    dag_id=f"dag_cosmos_report_{REPORT_NAME}",
    default_args=DEFAULT_ARGS,
    schedule=[Dataset("fct_data_completed")],
    catchup=False,
    dagrun_timeout=timedelta(minutes=60),
    description=f"Cosmos dbt report-centric pipeline for {REPORT_NAME}",
    tags=["dbt", "cosmos", "report-centric", REPORT_NAME],
)

with dag:
    start = EmptyOperator(task_id="start")

    report_flow = DbtTaskGroup(
        group_id=f"dbt_report_{REPORT_NAME}",
        project_config=project_config,
        profile_config=profile_config,
        execution_config=execution_config,
        render_config=RenderConfig(
            select=[f"+{REPORT_NAME}"],
        ),
    )

    end = EmptyOperator(
        task_id="end",
        outlets=[Dataset(f"report_{REPORT_NAME}_completed")],
        trigger_rule=DEFAULT_CHECK_DAG["trigger_rule"],
    )

    start >> report_flow >> end
