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


def build_cosmos_layer_group(
    layer_name: str,
    select_paths: list[str] | None = None,
    select_models: list[str] | None = None,
):
    """
    Tạo DbtTaskGroup cho một tầng dbt (dim, fct, bridge, reports, ...).

    Cosmos tự đọc ref() trong từng model và xếp thứ tự chạy đúng.
    Không cần khai báo dependency thủ công.

    Args:
        layer_name: Tên tầng, dùng để đặt group_id. Vd: "dim", "fct", "reports".
        select_paths: Danh sách selector dbt dạng path. Vd: ["path:models/dwh/dim"].
        select_models: Danh sách tên model cụ thể. Vd: ["dim_odoo_members", "dim_hc_snapshot_month"].
                       Uu tiên hơn select_paths. Cosmos vẫn tự xỻ lý dependency giữa các model này.
                       Nên dùng option này để chỉ chạy đúng các model cần thiết,
                       tránh chạy model cũ/bị tắt trong thư mục.
    """
    if select_models:
        select = select_models
    elif select_paths:
        select = select_paths
    else:
        select = [f"path:models/dwh/{layer_name}"]

    with TaskGroup(group_id=f"cosmos_{layer_name}_layer") as tg:

        dbt_group = DbtTaskGroup(
            group_id="dbt",
            project_config=project_config,
            profile_config=profile_config,
            execution_config=execution_config,
            render_config=RenderConfig(
                select=select,
                test_behavior="after_all",
            ),
        )

        # ── Monitoring sau khi toàn bộ layer chạy xong ──────────────────────
        # Đọc run_results.json một lần cho cả layer, tiết kiệm hơn
        # so với ghi log riêng từng bảng như dbt_factory.py cũ.
        success_log = save_success_log.override(
            task_id="success_log"
        )(layer_name)

        metrics = save_metrics.override(
            task_id="metrics"
        )(success_log, layer_name)

        failure_log = save_failure_log.override(
            task_id="failure_log"
        )(layer_name)

        failure_alert_task = failure_alert.override(
            task_id="failure_alert"
        )(layer_name)

        group_end = EmptyOperator(
            task_id="group_end",
            trigger_rule="none_failed",
        )

        dbt_group >> success_log >> metrics
        dbt_group >> failure_log
        dbt_group >> failure_alert_task
        [metrics, failure_log, failure_alert_task] >> group_end

    return tg


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