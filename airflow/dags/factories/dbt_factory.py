# factories/dbt_factory.py
"""
Factory để tạo DBT transformation tasks - TaskFlow API Version

Cải thiện so với version cũ:
- Dùng TaskFlow API (@task decorator) thay vì xcom_push/xcom_pull thủ công
- Truyền giá trị giữa tasks qua return value thay vì string path
- Loại bỏ hoàn toàn việc build full_task_id string dễ vỡ
- Tách biệt rõ ràng dependency giữa các tasks
"""
from __future__ import annotations

from airflow.sdk import TaskGroup
from airflow.decorators import task

SKIP_QC_TABLES = ['dim_date', 'fct_member_monthly_snapshot']  # Danh sách bảng không cần chạy quality check
from functools import lru_cache

@lru_cache(maxsize=None)
def _get_table_mapping(mapping_var: str) -> dict:
    from utils.mappings import (
        intermediate_mapping,
        report_mapping,
        dim_mapping,
        fct_mapping,
        snapshot_mapping,
        bridge_mapping,
    )

    mappings = {
        'intermediate_mapping': intermediate_mapping,
        'report_mapping': report_mapping,
        'dim_mapping': dim_mapping,
        'fct_mapping': fct_mapping,
        'snapshot_mapping': snapshot_mapping,
        'bridge_mapping': bridge_mapping,
    }
    return mappings.get(mapping_var, {})
def create_dbt_deps_task(dag):
    """Task chạy dbt deps, dùng chung cho tất cả DAG."""
    from airflow.providers.standard.operators.bash import BashOperator
    return BashOperator(
        task_id='dbt_deps',
        bash_command='cd /opt/airflow/dbt && dbt deps --profiles-dir /opt/airflow/dbt',
        dag=dag,
    )

def create_dbt_transformation_task_group(dag, source: str, pipeline_config: dict) -> TaskGroup:
    """
    Tạo TaskGroup chứa toàn bộ DBT transformation pipeline cho từng bảng.

    Flow mỗi bảng:
        dbt_run → success_logs → save_metrics
                ↘ failure_logs → failure_notification (trigger_rule=all_failed)

    Với SKIP_QC_TABLES:
        dbt_run → success_logs
                ↘ failure_logs → failure_notification
    """
    source_db = pipeline_config['source_db']
    target_db = pipeline_config['target_db']
    target_schema = pipeline_config.get('tgt_schema', 'public')
    dbt_target = pipeline_config['dbt_target']
    models_path = pipeline_config['models_path']

    table_mapping = _get_table_mapping(pipeline_config['table_mapping_var'])

    from config import DB_URIS, DBT_CONFIG
    from utils.common_tasks import _create_dbt_operator

    target_db_uri_fn = DB_URIS['dwh']
    task_group_prefix = f'{source}_to_{target_schema}'

    with TaskGroup(group_id=task_group_prefix, dag=dag) as outer_group:

        for tgt_table, _ in table_mapping.items():

            with TaskGroup(group_id=f'{source}_to_{tgt_table}', dag=dag):

                # ── 1. DBT Run ──────────────────────────────────────────────
                dbt_run = _create_dbt_operator(
                    task_id=f'dbt_{target_schema}_{tgt_table}',
                    mapping_var=pipeline_config['table_mapping_var'],
                    models_path=models_path,
                    target_schema=target_schema,
                    tgt_table=tgt_table,
                    dag=dag,
                    dbt_target=dbt_target,
                    DBT_CONFIG=DBT_CONFIG,
                )

                # ── 2. Success logs → trả về job_id ────────────────────────
                @task(task_id=f'success_save_logs_{target_schema}_{tgt_table}', dag=dag)
                def save_success_logs(
                    _src=source,
                    _tgt_table=tgt_table,
                    _source_db=source_db,
                    _target_db=target_db,
                ) -> int:
                    """Lưu job log thành công, return job_id cho các task tiếp theo."""
                    from config import DB_URIS, get_local_now
                    from utils.monitoring import save_job_log
                    from airflow.operators.python import get_current_context
                    import json
                    context = get_current_context()

                    log_data = {
                        'job_name': f'to_{_target_db}',
                        'source_db': _source_db,
                        'target_db': _target_db,
                        'source_table': json.dumps(_src),
                        'target_table': _tgt_table,
                        'status': 'SUCCESS',
                        'execution_time': get_local_now(),
                        'dag_id': context['dag'].dag_id,
                        'task_id': context['task'].task_id, 
                    }
                    log_uri = DB_URIS['monitoring']()
                    job_id = save_job_log(log_uri, log_data)
                    print(f'[save_logs] job_id={job_id} table={_tgt_table}')
                    return job_id  # TaskFlow tự push XCom

                # ── 3. Save metrics — nhận job_id trực tiếp ────────────────
                @task(task_id=f'metrics_{target_schema}_{tgt_table}', dag=dag)
                def save_metrics(
                    job_id: int,  # nhận từ save_success_logs qua TaskFlow
                    _src=source,
                    _tgt_table=tgt_table,
                    _target_schema=target_schema,
                    _target_db_uri_fn=target_db_uri_fn,
                ) -> None:
                    """Lưu metrics sau khi DBT chạy xong."""
                    from config import DB_URIS, get_local_now, to_local_datetime
                    from utils.monitoring import save_metrics as _save_metrics
                    from sqlalchemy import create_engine, text
                    from airflow.operators.python import get_current_context

                    if not job_id:
                        print(f'[save_metrics] Warning: không có job_id cho {_tgt_table}')
                        return

                    # Lấy dbt execution time từ Airflow task instance
                    # (thay vì đọc run_results.json — tránh race condition trên đa worker)
                    execution_time = 0
                    try:
                        ctx = get_current_context()
                        dbt_task_key = f'dbt_{_target_schema}_{_tgt_table}'
                        for ti in ctx['dag_run'].get_task_instances():
                            if ti.task_id.endswith(dbt_task_key) and ti.duration:
                                execution_time = round(ti.duration, 2)
                                break
                    except Exception as e:
                        print(f'[save_metrics] Could not get dbt task duration: {e}')

                    target_db_uri = (
                        _target_db_uri_fn()
                        if callable(_target_db_uri_fn)
                        else _target_db_uri_fn
                    )
                    engine = create_engine(target_db_uri)
                    with engine.connect() as conn:
                        row_count = conn.execute(
                            text(f'SELECT COUNT(*) FROM {_target_schema}.{_tgt_table}')
                        ).scalar()

                        has_etl_datetime = conn.execute(
                            text(
                                """
                                SELECT EXISTS (
                                    SELECT 1
                                    FROM information_schema.columns
                                    WHERE table_schema = :table_schema
                                      AND table_name = :table_name
                                      AND column_name = 'etl_datetime'
                                )
                                """
                            ),
                            {
                                'table_schema': _target_schema,
                                'table_name': _tgt_table,
                            },
                        ).scalar()

                        max_updated_at = None
                        if has_etl_datetime:
                            max_updated_at = conn.execute(
                                text(f'SELECT MAX(etl_datetime) FROM {_target_schema}.{_tgt_table}')
                            ).scalar()

                    engine.dispose()

                    data_delay_minutes = None
                    if max_updated_at:
                        now = get_local_now()
                        max_updated_at = to_local_datetime(max_updated_at)
                        data_delay_minutes = int(
                            (now - max_updated_at).total_seconds() / 60
                        )

                    metrics = {
                        'total_duration': execution_time,
                        'target_row_count': row_count,
                        'max_updated_at': max_updated_at,
                        'data_delay_minutes': data_delay_minutes,
                        'source_row_count': None,
                        'extract_duration': None,
                        'load_duration': None,
                        'peak_memory_mb': None,
                    }

                    log_uri = DB_URIS['monitoring']()
                    _save_metrics(log_uri, job_id, metrics)
                    print(f'[save_metrics] Saved metrics for job_id={job_id}, table={_tgt_table}')

                # ── 5. Failure logs ─────────────────────────────────────────
                @task(
                    task_id=f'failure_save_logs_{target_schema}_{tgt_table}',
                    dag=dag,
                    trigger_rule='all_failed',
                )
                def save_failure_logs(
                    _src=source,
                    _tgt_table=tgt_table,
                    _source_db=source_db,
                    _target_db=target_db,
                ) -> None:
                    """Lưu job log khi có lỗi."""
                    from config import DB_URIS, get_local_now
                    from utils.monitoring import save_job_log
                    from airflow.operators.python import get_current_context
                    import json

                    context = get_current_context()
                    log_data = {
                        'job_name': f'to_{_target_db}',
                        'source_db': _source_db,
                        'target_db': _target_db,
                        'source_table': json.dumps(_src),
                        'target_table': _tgt_table,
                        'status': 'FAILURE',
                        'execution_time': get_local_now(),
                        'dag_id': context['dag'].dag_id,
                        'task_id': context['task'].task_id,
                    }
                    log_uri = DB_URIS['monitoring']()
                    save_job_log(log_uri, log_data)
                    print(f'[failure_logs] Saved failure log for {_tgt_table}')

                @task(
                    task_id=f'failure_notification_{target_schema}_{tgt_table}',
                    dag=dag,
                    trigger_rule='all_failed',
                )
                def send_failure_notification(
                    _src=source,
                    _tgt_table=tgt_table,
                    _source_db=source_db,
                    _target_db=target_db,
                    _target_schema=target_schema,
                ) -> None:
                    """Gửi Telegram notification chỉ khi DBT task thất bại."""
                    from airflow.operators.python import get_current_context
                    from config import get_telegram_config, get_local_now
                    from utils.telegram_notification import send_telegram_message
                    import html

                    context = get_current_context()
                    ti = context['ti']
                    run_id = context['dag_run'].run_id
                    log_url = ti.log_url
                    execution_time = get_local_now().format('YYYY-MM-DD HH:mm:ss')

                    telegram_config = get_telegram_config()
                    message = (
                        '❌ <b>DBT task failed</b>\n'
                        f'<b>Source</b>: {html.escape(str(_src))}\n'
                        f'<b>Target DB</b>: {html.escape(str(_target_db))}\n'
                        f'<b>Target Table</b>: {html.escape(str(_target_schema))}.{html.escape(str(_tgt_table))}\n'
                        f'<b>DAG</b>: {html.escape(str(context["dag"].dag_id))}\n'
                        f'<b>Run ID</b>: {html.escape(str(run_id))}\n'
                        f'<b>Execution Time</b>: {execution_time}\n'
                        f'<b>Log</b>: <a href="{html.escape(str(log_url))}">View Log</a>'
                    )
                    send_telegram_message(
                        message=message,
                        bot_token=telegram_config['bot_token'],
                        chat_id=telegram_config['chat_id']
                    )
                    print(f'[failure_notification] Sent Telegram failure alert for {_tgt_table}')

                # ── Wire dependencies ───────────────────────────────────────
                logs = save_success_logs()
                failure = save_failure_logs()
                failure_notification = send_failure_notification()

                if tgt_table in SKIP_QC_TABLES:
                    # Skip QC: dbt → success_logs
                    dbt_run >> logs
                else:
                    # Full flow: dbt → success_logs → metrics
                    metrics_result = save_metrics(logs)
                    dbt_run >> logs >> metrics_result

                # Failure branch: logs trước, notification sau
                dbt_run >> failure >> failure_notification

    return outer_group
