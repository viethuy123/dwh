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
from airflow.providers.standard.operators.python import PythonOperator

SKIP_QC_TABLES = ['dim_date', 'fct_member_monthly_snapshot']
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


def create_dbt_transformation_task_group(dag, source: str, pipeline_config: dict) -> TaskGroup:
    """
    Tạo TaskGroup chứa toàn bộ DBT transformation pipeline cho từng bảng.

    Flow mỗi bảng:
        dbt_run → success_logs → save_metrics → quality_check → notification
                ↘ failure_logs (trigger_rule=all_failed)

    Với SKIP_QC_TABLES:
        dbt_run → success_logs
                ↘ failure_logs
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
                    from config import DB_URIS
                    from utils.monitoring import save_job_log
                    from datetime import datetime
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
                        'execution_time': datetime.utcnow(),
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
                    from config import DB_URIS, DBT_CONFIG
                    from utils.monitoring import save_metrics as _save_metrics
                    from sqlalchemy import create_engine, text
                    from datetime import datetime
                    import os, json

                    if not job_id:
                        print(f'[save_metrics] Warning: không có job_id cho {_tgt_table}')
                        return

                    dbt_project_dir = DBT_CONFIG['project_dir']
                    target_name = DBT_CONFIG['target_name']
                    run_results_path = os.path.join(
                        dbt_project_dir, 'target', target_name, 'run_results.json'
                    )

                    execution_time = 0
                    if os.path.exists(run_results_path):
                        with open(run_results_path) as f:
                            run_results = json.load(f)
                        for r in run_results.get('results', []):
                            if r['unique_id'].endswith(_tgt_table):
                                execution_time = r.get('execution_time', 0)
                                break
                    else:
                        print(f'[save_metrics] Warning: run_results.json không tìm thấy')

                    engine = create_engine(DB_URIS['dwh']())
                    with engine.connect() as conn:
                        row_count = conn.execute(
                            text(f'SELECT COUNT(*) FROM {_target_schema}.{_tgt_table}')
                        ).scalar()

                        max_updated_at = conn.execute(
                            text(f'SELECT MAX(etl_datetime) FROM {_target_schema}.{_tgt_table}')
                        ).scalar()

                    data_delay_minutes = None
                    if max_updated_at:
                        now = datetime.utcnow()
                        data_delay_minutes = int(
                            (now - max_updated_at.replace(tzinfo=None)).total_seconds() / 60
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

                # ── 4. Quality check → trả về validation result ─────────────
                @task(task_id=f'{target_schema}_{tgt_table}_quality_check', dag=dag)
                def quality_check(
                    _tgt_table=tgt_table,
                    _target_schema=target_schema,
                    _target_db_uri_fn=target_db_uri_fn,
                ) -> dict:
                    """Chạy data quality check, return kết quả cho notification."""
                    from utils.extract_data import extract_sql_data
                    from utils.data_quality import validate_dataframe

                    uri = _target_db_uri_fn() if callable(_target_db_uri_fn) else _target_db_uri_fn
                    full_table = f'{_target_schema}.{_tgt_table}'
                    suite_name = f'{_target_schema}_{_tgt_table}'

                    data = extract_sql_data(uri, f'SELECT * FROM {full_table} LIMIT 1000')
                    result = validate_dataframe(df=data, suite_name=suite_name)
                    trimmed = {
                        'success': result.get('success'),
                        'suite_name': result.get('suite_name'),
                        'statistics': result.get('statistics'),
                        'results': [          # ← giữ nguyên key 'results' để không vỡ downstream
                            {
                                'success': r.get('success'),
                                'expectation_config': {
                                    'type': r['expectation_config']['type'],
                                    'kwargs': {'column': r['expectation_config']['kwargs'].get('column')},
                                },
                                'result': r.get('result'),
                            }
                            for r in result.get('results', [])
                            if not r.get('success')  # chỉ giữ failed
                        ],
                    }
                    return trimmed

                # ── 5. Notification — nhận validation_result trực tiếp ──────
                @task(task_id=f'{target_schema}_{tgt_table}_notification', dag=dag)
                def notification(
                    validation_result: dict,  # nhận từ quality_check qua TaskFlow
                    _src=source,
                    _tgt_table=tgt_table,
                    _target_schema=target_schema,
                    _target_db_uri_fn=target_db_uri_fn,
                ) -> None:
                    """Gửi notification lên Slack với kết quả validation."""
                    from airflow.sdk import Variable
                    from utils.extract_data import extract_sql_data
                    from utils.data_quality_notification import send_validation_results
                    from config import get_slack_config

                    uri = _target_db_uri_fn() if callable(_target_db_uri_fn) else _target_db_uri_fn
                    suite_name = f'{_target_schema}_{_tgt_table}'
                    full_table = f'{_target_schema}.{_tgt_table}'

                    total_rows = extract_sql_data(
                        uri, f'SELECT count(*) as total_rows FROM {full_table}'
                    )['total_rows'][0]

                    prev_rows = int(Variable.get(f'{suite_name}_prev_rows', default=0))
                    new_rows_inserted = total_rows - prev_rows

                    slack_config = get_slack_config()
                    send_validation_results(
                        table_name=suite_name,
                        validation_result=validation_result,
                        slack_channel_id=slack_config['chat_id'],
                        slack_bot_token=slack_config['bot_token'],
                        total_rows=total_rows,
                        new_rows_inserted=new_rows_inserted,
                    )

                    Variable.set(f'{suite_name}_prev_rows', str(total_rows))
                    print(f'[notification] Sent notification for {suite_name}')

                # ── 6. Failure logs ─────────────────────────────────────────
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
                    from config import DB_URIS
                    from utils.monitoring import save_job_log
                    from datetime import datetime
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
                        'execution_time': datetime.utcnow(),
                        'dag_id': context['dag'].dag_id,
                        'task_id': context['task'].task_id,
                    }
                    log_uri = DB_URIS['monitoring']()
                    save_job_log(log_uri, log_data)
                    print(f'[failure_logs] Saved failure log for {_tgt_table}')

                # ── Wire dependencies ───────────────────────────────────────
                logs = save_success_logs()
                failure = save_failure_logs()

                if tgt_table in SKIP_QC_TABLES:
                    # Skip QC: dbt → success_logs
                    dbt_run >> logs
                else:
                    # Full flow: dbt → success_logs → metrics → quality_check → notification
                    metrics_result = save_metrics(logs)
                    qc_result = quality_check()
                    notif = notification(qc_result)
                    dbt_run >> logs >> metrics_result >> qc_result >> notif

                # Failure logs luôn chạy khi dbt fail
                dbt_run >> failure

    return outer_group