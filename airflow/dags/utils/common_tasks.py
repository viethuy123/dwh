# utils/common_tasks.py
"""
Chứa các helper dùng chung trong nhiều DAGs.

Cải thiện so với version cũ:
- Loại bỏ create_data_quality_check_callable và create_data_notification_callable
  → Logic đã được inline vào @task functions trong dbt_factory.py và ingestion_factory.py
  → Không còn truyền giá trị qua xcom_pull với string path dài dễ vỡ

- Loại bỏ create_save_job_logs_callable và create_save_metrics_callable
  → Cũng đã được inline vào @task functions trong từng factory
  → Giá trị job_id được truyền trực tiếp qua TaskFlow return value

- Giữ lại:
  - _create_dbt_operator: tạo DbtRunOperator / DbtSnapshotOperator
  - sync_fdw_tables: dùng chung cho nhiều pipelines
"""
from __future__ import annotations

from sqlalchemy import create_engine, text
from airflow_dbt_python.operators.dbt import DbtRunOperator, DbtSnapshotOperator


# ─────────────────────────────────────────────────────────────────────────────
# DBT Operator Factory
# ─────────────────────────────────────────────────────────────────────────────

def _create_dbt_operator(
    *,
    task_id: str,
    mapping_var: str,
    models_path: str,
    target_schema: str,
    tgt_table: str,
    dag,
    dbt_target: str,
    DBT_CONFIG: dict,
):
    """
    Tạo DbtRunOperator hoặc DbtSnapshotOperator tuỳ theo mapping_var.

    Args:
        task_id: Task ID cho operator
        mapping_var: Tên mapping var để xác định loại operator
        models_path: Đường dẫn đến thư mục models DBT
        target_schema: Schema đích (vd: 'intermediates', 'dim', 'fct')
        tgt_table: Tên bảng đích
        dag: DAG instance
        dbt_target: DBT target profile (vd: 'dev', 'prod')
        DBT_CONFIG: Dict chứa project_dir, profiles_dir, profile
    """
    common_kwargs = dict(
        task_id=task_id,
        project_dir=DBT_CONFIG['project_dir'],
        profiles_dir=DBT_CONFIG['profiles_dir'],
        target=dbt_target,
        profile=DBT_CONFIG['profile'],
        upload_dbt_project=True,
        dag=dag,
    )

    if mapping_var == 'snapshot_mapping':
        return DbtSnapshotOperator(**common_kwargs)

    return DbtRunOperator(
        **common_kwargs,
        select=[f'path:{models_path}/{target_schema}/{tgt_table}.sql'],
    )


# ─────────────────────────────────────────────────────────────────────────────
# FDW Sync
# ─────────────────────────────────────────────────────────────────────────────

def sync_fdw_tables(
    tgt_schema: str,
    src_schema: str,
    server_name: str,
    db_uri_fn,
) -> None:
    """
    Sync FDW tables — dùng chung cho tất cả pipelines.

    Args:
        tgt_schema: Target schema name
        src_schema: Source schema name
        server_name: FDW server name
        db_uri_fn: Callable hoặc string URI của database
    """
    db_uri = db_uri_fn() if callable(db_uri_fn) else db_uri_fn
    engine = create_engine(db_uri)

    with engine.connect() as conn:
        conn.execute(
            text(
                f"CALL public.sync_fdw_tables('{tgt_schema}', '{src_schema}', '{server_name}');"
                "commit;"
            )
        )

    print(f'[sync_fdw_tables] Synced {src_schema} → {tgt_schema} via {server_name}')
    return None