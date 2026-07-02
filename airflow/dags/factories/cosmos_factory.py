"""Factory for report-centric dbt Cosmos DAGs."""
from __future__ import annotations
from airflow.sdk import TaskGroup


from cosmos import DbtTaskGroup, ExecutionConfig, ProfileConfig, ProjectConfig, RenderConfig
from airflow.providers.standard.operators.empty import EmptyOperator

from config import DBT_EXECUTABLE_PATH, DBT_PROFILES_DIR, DBT_PROJECT_DIR

from utils.report_monitoring import (
    save_success_log,
    save_metrics,
    save_failure_log,
    failure_alert,
)
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


# def build_report_task_group(report_name: str):

#     with TaskGroup(
#         group_id=f"report_{report_name}"
#     ) as tg:

#         dbt_group = DbtTaskGroup(
#             group_id="dbt",
#             project_config=project_config,
#             profile_config=profile_config,
#             execution_config=execution_config,
#             render_config=RenderConfig(
#                 select=[f"+{report_name}"],
#                 test_behavior="after_all",
#             ),
#         )

#         success_log = save_success_log.override(
#         task_id="success_log"
#         )(report_name)

#         metrics = save_metrics.override(
#             task_id="metrics"
#         )(
#             success_log,
#             report_name,
#         )

#         failure_log = save_failure_log.override(
#             task_id="failure_log"
#         )(report_name)

#         failure_alert_task = failure_alert.override(
#             task_id="failure_alert"
#         )(report_name)

#         dbt_group >> success_log >> metrics

#         dbt_group >> failure_log
#         dbt_group >> failure_alert_task

#     return tg

def build_report_task_group(report_name: str):
    with TaskGroup(group_id=f"report_{report_name}") as tg:

        dbt_group = DbtTaskGroup(
            group_id="dbt",
            project_config=project_config,
            profile_config=profile_config,
            execution_config=execution_config,
            render_config=RenderConfig(
                select=[f"+{report_name}"],
                test_behavior="after_all",
            ),
        )

        success_log = save_success_log.override(task_id="success_log")(report_name)
        metrics = save_metrics.override(task_id="metrics")(success_log, report_name)

        failure_log = save_failure_log.override(task_id="failure_log")(report_name)
        failure_alert_task = failure_alert.override(task_id="failure_alert")(report_name)

        dbt_group >> success_log >> metrics
        dbt_group >> failure_log
        dbt_group >> failure_alert_task

        group_end = EmptyOperator(
            task_id="group_end",
            trigger_rule="none_failed",
        )
        # PHẢI gồm đủ cả 3 leaf gốc: metrics, failure_log, failure_alert_task
        [metrics, failure_log, failure_alert_task] >> group_end

    return tg