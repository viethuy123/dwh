# factories/ingestion_factory.py
"""
Factory để tạo ingestion tasks - TaskFlow API Version

Cải thiện so với version cũ:
- Dùng TaskFlow API (@task decorator) thay vì PythonOperator + xcom_push/xcom_pull thủ công
- Truyền job_id, metrics, validation_result qua return value thay vì string path
- Loại bỏ hoàn toàn create_data_quality_check_callable, create_data_notification_callable,
  create_save_job_logs_callable, create_save_metrics_callable từ common_tasks
- _create_extract_load_callable giữ nguyên logic chunking/full load,
  nhưng return metrics thay vì xcom_push thủ công
"""
from __future__ import annotations

import csv
import gc
from io import StringIO

from airflow.sdk import TaskGroup
from airflow.decorators import task


# ─────────────────────────────────────────────────────────────────────────────
# Helper: PostgreSQL COPY insert
# ─────────────────────────────────────────────────────────────────────────────

def psql_insert_copy(table, conn, keys, data_iter):
    """Dùng lệnh COPY của Postgres thay vì INSERT — nhanh hơn nhiều."""
    dbapi_conn = conn.connection
    with dbapi_conn.cursor() as cur:
        s_buf = StringIO()
        writer = csv.writer(s_buf)
        writer.writerows(data_iter)
        s_buf.seek(0)

        columns = ', '.join('"{}"'.format(k) for k in keys)
        table_name = (
            '{}.{}'.format(table.schema, table.name) if table.schema else table.name
        )
        cur.copy_expert(
            sql='COPY {} ({}) FROM STDIN WITH CSV'.format(table_name, columns),
            file=s_buf,
        )


# ─────────────────────────────────────────────────────────────────────────────
# Helper: Load chunk vào Postgres
# ─────────────────────────────────────────────────────────────────────────────

def _load_chunk(df_chunk, pg_engine, tgt_table: str, target_schema: str, load_mode: str, dtype: dict = None):
    with pg_engine.begin() as pg_conn:
        df_chunk.to_sql(
            tgt_table,
            con=pg_conn,
            if_exists=load_mode,
            index=False,
            schema=target_schema,
            dtype=dtype,
            method=psql_insert_copy,
        )


# ─────────────────────────────────────────────────────────────────────────────
# Load: Chunking mode
# ─────────────────────────────────────────────────────────────────────────────

def _load_with_chunking(
    source_uri: str,
    source_type: str,
    src_table: str,
    pg_engine,
    tgt_table: str,
    target_schema: str,
    transformer,
    chunk_size: int,
    source_db: str = None,
    order_by: str = None,
    extract_query: str = None,
) -> int:
    from utils.extract_data import extract_mongo_data_chunked, extract_sql_data_chunked
    from utils.data_transformers import transform_dataframe, add_columns_to_table

    if source_type == 'mongodb':
        chunk_iterator = extract_mongo_data_chunked(source_uri, source_db, src_table, chunk_size)
    else:
        query = (extract_query or '').strip().rstrip(';')
        if not query:
            query = f'SELECT * FROM {src_table}'
            if order_by:
                query = f'{query} ORDER BY {order_by}'

        chunk_iterator = extract_sql_data_chunked(
            source_uri,
            query,
            chunk_size,
        )

    is_first_chunk = True
    processed_rows = 0
    known_columns: set = set()

    for i, df_chunk in enumerate(chunk_iterator):

        # MongoDB: ép tất cả sang string (trừ etl_datetime)
        if source_type == 'mongodb':
            for col in df_chunk.columns:
                if col != 'etl_datetime':
                    df_chunk[col] = (
                        df_chunk[col].astype(str).replace('nan', None).replace('None', None)
                    )

        df_chunk = transform_dataframe(df_chunk, transformer)

        if is_first_chunk:
            if source_type == 'mongodb':
                from sqlalchemy.types import TEXT
                dtype_dict = {col: TEXT for col in df_chunk.columns if col != 'etl_datetime'}
                _load_chunk(df_chunk, pg_engine, tgt_table, target_schema, 'replace', dtype_dict)
            else:
                _load_chunk(df_chunk, pg_engine, tgt_table, target_schema, 'replace')

            known_columns = set(df_chunk.columns)
            is_first_chunk = False
        else:
            new_columns = set(df_chunk.columns) - known_columns
            if new_columns:
                print(f'[chunking] New columns detected: {new_columns}')
                add_columns_to_table(pg_engine, tgt_table, target_schema, new_columns)
                known_columns.update(new_columns)

            # Đảm bảo đủ cột và đúng thứ tự
            for col in known_columns - set(df_chunk.columns):
                df_chunk[col] = None
            df_chunk = df_chunk[sorted(known_columns)]

            _load_chunk(df_chunk, pg_engine, tgt_table, target_schema, 'append')

        processed_rows += len(df_chunk)
        print(f'✓ Chunk {i + 1}: {len(df_chunk)} rows | Total: {processed_rows}')

        del df_chunk
        gc.collect()

    print(f'[chunking] Completed: {processed_rows} rows from {source_type}')
    return processed_rows


# ─────────────────────────────────────────────────────────────────────────────
# Load: Full load mode
# ─────────────────────────────────────────────────────────────────────────────

def _load_full_table(
    source_uri: str,
    source_type: str,
    source_db: str,
    src_table: str,
    pg_engine,
    tgt_table: str,
    target_schema: str,
    transformer,
    batch_size: int = 5000,
) -> int:
    from utils.extract_data import extract_sql_data, extract_mongo_data
    from utils.data_transformers import transform_dataframe
    import pandas as pd

    df = None
    try:
        if source_type == 'mongodb':
            df = extract_mongo_data(source_uri, source_db, src_table)
        else:
            df = extract_sql_data(source_uri, f'SELECT * FROM {src_table}')

        if df is None or df.empty:
            print(f'[full_load] Source table {src_table} is empty, skipping')
            return 0

        total_rows = len(df)
        print(f'[full_load] Extracted {total_rows} rows from {src_table}')

        # Xử lý dtype
        for col in df.columns:
            if df[col].isnull().all():
                df[col] = df[col].astype(object)
            if (
                any(k in col.lower() for k in ['date', 'time', 'at'])
                and df[col].dtype != 'object'
            ):
                df[col] = df[col].astype(object)

        df = transform_dataframe(df, transformer)

        # Batch insert
        for batch_num, start_idx in enumerate(range(0, total_rows, batch_size)):
            end_idx = min(start_idx + batch_size, total_rows)
            df_batch = df.iloc[start_idx:end_idx].copy()
            load_mode = 'replace' if batch_num == 0 else 'append'

            with pg_engine.begin() as conn:
                df_batch.to_sql(
                    tgt_table,
                    con=conn,
                    if_exists=load_mode,
                    index=False,
                    schema=target_schema,
                    method=psql_insert_copy,
                )

            print(f'✓ Batch {batch_num + 1}: {len(df_batch)} rows | Progress: {end_idx}/{total_rows}')
            del df_batch
            gc.collect()

        print(f'[full_load] Completed: {total_rows} rows')
        return total_rows

    finally:
        if df is not None:
            del df
        for _ in range(3):
            gc.collect()


# ─────────────────────────────────────────────────────────────────────────────
# Extract & Load callable — return metrics thay vì xcom_push thủ công
# ─────────────────────────────────────────────────────────────────────────────

def _create_extract_load_callable(
    source_uri_fn,
    target_schema: str,
    source_type: str,
    source_db: str = None,
):
    """
    Tạo callable cho extract + load.

    Return: dict metrics (được TaskFlow tự push XCom khi dùng trong @task).
    """

    def extract_load_data(
        src_table: str,
        tgt_table: str,
        chunk_size: int = None,
        order_by: str = None,
        extract_query: str = None,
    ) -> dict:
        from sqlalchemy import create_engine, text, pool
        from config import DB_URIS
        from utils.data_transformers import get_transformer
        from utils.monitoring import ETLMonitor

        monitor = ETLMonitor()
        pg_engine = None

        try:
            source_uri = source_uri_fn()
            dwh_uri = DB_URIS['dwh']()

            pg_engine = create_engine(dwh_uri, poolclass=pool.NullPool, echo=False)
            transformer = get_transformer(source_type)

            with pg_engine.begin() as conn:
                conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS {target_schema}'))

            print(f'[extract_load] Start: {src_table} | mode: {"chunk=" + str(chunk_size) if chunk_size else "full_load"}')

            # if chunk_size:
            #     total_rows = _load_with_chunking(
            #         source_uri, source_type, src_table,
            #         pg_engine, tgt_table, target_schema,
            #         transformer, chunk_size, source_db,
            #     )
            # else:
            #     total_rows = _load_full_table(
            #         source_uri, source_type, source_db,
            #         src_table, pg_engine, tgt_table,
            #         target_schema, transformer,
            #     )
            DEFAULT_CHUNK_SIZE = 10_000

            total_rows = _load_with_chunking(
                source_uri, source_type, src_table,
                pg_engine, tgt_table, target_schema,
                transformer,
                chunk_size=chunk_size or DEFAULT_CHUNK_SIZE,  # fallback nếu không truyền
                source_db=source_db,
                order_by=order_by,
                extract_query=extract_query,
            )

            monitor.record_load(total_rows)
            metrics = monitor.finalize()

            # Return metrics — TaskFlow tự push XCom, không cần get_current_context()
            return metrics

        except Exception as e:
            print(f'[extract_load] ERROR: {e}')
            raise

        finally:
            if pg_engine:
                pg_engine.dispose()
            for _ in range(3):
                gc.collect()

    return extract_load_data


# ─────────────────────────────────────────────────────────────────────────────
# Main factory
# ─────────────────────────────────────────────────────────────────────────────

def create_ingestion_task_group(dag, source: str, ingestion_config: dict) -> TaskGroup:
    """
    Tạo TaskGroup chứa toàn bộ ingestion pipeline cho từng bảng.

    Flow mỗi bảng:
        extract_load → success_logs → save_metrics → quality_check → notification
                     ↘ failure_logs (trigger_rule=all_failed)
    """
    source_type = ingestion_config['source_type']
    source_uri_fn = ingestion_config['source_uri_fn']
    source_db = ingestion_config['source_db']
    target_schema = ingestion_config['target_schema']
    tables = ingestion_config['tables']

    from config import DB_URIS, get_pool_name, get_target_table_name

    target_db_uri_fn = DB_URIS['dwh']
    task_group_prefix = f'{source}_to_{target_schema}'

    extract_load_fn = _create_extract_load_callable(
        source_uri_fn=source_uri_fn,
        target_schema=target_schema,
        source_type=source_type,
        source_db=source_db,
    )

    with TaskGroup(group_id=task_group_prefix, dag=dag) as outer_group:

        for table_config in tables:
            src_table = table_config['name']
            tgt_table = get_target_table_name(src_table, source)
            table_type = table_config['type']
            chunk_size = table_config.get('chunksize')
            order_by = table_config.get('order_by')
            extract_query = table_config.get('extract_query')
            pool_name = get_pool_name(table_type)

            with TaskGroup(group_id=f'{source}_to_{tgt_table}', dag=dag):

                # ── 1. Extract & Load → return metrics ──────────────────────
                @task(
                    task_id=f'extract_load_{target_schema}_{tgt_table}',
                    dag=dag,
                    pool=pool_name,
                )
                def extract_load(
                    _src_table=src_table,
                    _tgt_table=tgt_table,
                    _chunk_size=chunk_size,
                    _order_by=order_by,
                    _extract_query=extract_query,
                    _fn=extract_load_fn,
                ) -> dict:
                    """Extract từ source, load vào DWH. Return metrics dict."""
                    return _fn(
                        src_table=_src_table,
                        tgt_table=_tgt_table,
                        chunk_size=_chunk_size,
                        order_by=_order_by,
                        extract_query=_extract_query,
                    )

                # ── 2. Success logs → return job_id ─────────────────────────
                @task(task_id=f'success_save_logs_{target_schema}_{tgt_table}', dag=dag)
                def save_success_logs(
                    _src=source,
                    _tgt_table=tgt_table,
                    _target_schema=target_schema,
                ) -> int:
                    """Lưu job log thành công, return job_id."""
                    from config import DB_URIS
                    from utils.monitoring import save_job_log
                    from datetime import datetime
                    from airflow.operators.python import get_current_context
                    import json
                    context = get_current_context()

                    log_data = {
                        'job_name': f'to_dwh',
                        'source_db': _src,
                        'target_db': 'dwh',
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

                # ── 3. Save metrics — nhận job_id + metrics trực tiếp ───────
                @task(task_id=f'metrics_{target_schema}_{tgt_table}', dag=dag)
                def save_metrics(
                    job_id: int,
                    metrics: dict,  # nhận từ extract_load qua TaskFlow
                    _tgt_table=tgt_table,
                    _target_schema=target_schema,
                    _uri_fn=target_db_uri_fn,
                ) -> None:
                    """Lưu metrics vào monitoring DB."""
                    from config import DB_URIS
                    from utils.monitoring import save_metrics as _save_metrics
                    from sqlalchemy import create_engine, text
                    from datetime import datetime

                    if not job_id or not metrics:
                        print(f'[save_metrics] Warning: thiếu job_id hoặc metrics cho {_tgt_table}')
                        return

                    # Bổ sung max_updated_at và data_delay_minutes từ DB
                    # vì ETLMonitor không track các field này
                    engine = create_engine(_uri_fn())
                    with engine.connect() as conn:
                        max_updated_at = conn.execute(
                            text(f'SELECT MAX(etl_datetime) FROM {_target_schema}.{_tgt_table}')
                        ).scalar()

                    data_delay_minutes = None
                    if max_updated_at:
                        data_delay_minutes = int(
                            (datetime.utcnow() - max_updated_at.replace(tzinfo=None)).total_seconds() / 60
                        )

                    metrics['max_updated_at'] = max_updated_at
                    metrics['data_delay_minutes'] = data_delay_minutes

                    # Đảm bảo các field nullable có mặt đủ để SQL không báo thiếu bind parameter
                    metrics.setdefault('source_row_count', None)
                    metrics.setdefault('extract_duration', None)

                    log_uri = DB_URIS['monitoring']()
                    _save_metrics(log_uri, job_id, metrics)
                    print(f'[save_metrics] Saved for job_id={job_id}, table={_tgt_table}')

                # ── 4. Quality check → return validation result ──────────────
                @task(task_id=f'{target_schema}_{tgt_table}_quality_check', dag=dag)
                def quality_check(
                    _tgt_table=tgt_table,
                    _target_schema=target_schema,
                    _uri_fn=target_db_uri_fn,
                ) -> dict:
                    """Chạy data quality check, return kết quả."""
                    from utils.extract_data import extract_sql_data
                    from utils.data_quality import validate_dataframe

                    uri = _uri_fn() if callable(_uri_fn) else _uri_fn
                    suite_name = f'{_target_schema}_{_tgt_table}'
                    data = extract_sql_data(
                        uri, f'SELECT * FROM {_target_schema}.{_tgt_table} LIMIT 1000'
                    )
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
                    _uri_fn=target_db_uri_fn,
                ) -> None:
                    """Gửi Slack notification với kết quả validation."""
                    from airflow.sdk import Variable
                    from utils.extract_data import extract_sql_data
                    from utils.data_quality_notification import send_validation_results
                    from config import get_slack_config

                    uri = _uri_fn() if callable(_uri_fn) else _uri_fn
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

                # ── 6. Failure logs ──────────────────────────────────────────
                @task(
                    task_id=f'failure_save_logs_{target_schema}_{tgt_table}',
                    dag=dag,
                    trigger_rule='all_failed',
                )
                def save_failure_logs(
                    _src=source,
                    _tgt_table=tgt_table,
                ) -> None:
                    """Lưu job log khi có lỗi."""
                    from config import DB_URIS
                    from utils.monitoring import save_job_log
                    from datetime import datetime
                    from airflow.operators.python import get_current_context
                    import json
                    context = get_current_context()
                    log_data = {
                        'job_name': 'to_dwh',
                        'source_db': _src,
                        'target_db': 'dwh',
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

                # ── Wire dependencies ────────────────────────────────────────
                el_result = extract_load()          # return metrics dict
                logs = save_success_logs()          # return job_id
                failure = save_failure_logs()

                # metrics nhận cả job_id lẫn metrics từ extract_load — không cần XCom string
                metrics_task = save_metrics(job_id=logs, metrics=el_result)
                qc_result = quality_check()
                notif = notification(qc_result)

                el_result >> logs >> metrics_task >> qc_result >> notif
                el_result >> failure  # failure log chạy khi extract_load fail

    return outer_group
