# utils/common_tasks.py
"""
Chứa các task callables được dùng chung trong nhiều DAGs
"""
from airflow.sdk import Variable
from utils.extract_data import extract_sql_data
from utils.data_quality import validate_dataframe
from utils.data_quality_notification import send_validation_results
from utils.etl_job_logs import save_etl_job_logs
from sqlalchemy import create_engine, text
from airflow_dbt_python.operators.dbt import DbtRunOperator, DbtSnapshotOperator
from datetime import datetime
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
    if mapping_var == "snapshot_mapping":
        return DbtSnapshotOperator(
            task_id=task_id,
            project_dir=DBT_CONFIG["project_dir"],
            profiles_dir=DBT_CONFIG["profiles_dir"],
            target=dbt_target,
            profile=DBT_CONFIG["profile"],
            upload_dbt_project=True,
            dag=dag,
        )
    else:
        return DbtRunOperator(
            task_id=task_id,
            project_dir=DBT_CONFIG["project_dir"],
            profiles_dir=DBT_CONFIG["profiles_dir"],
            select=[f"path:{models_path}/{target_schema}/{tgt_table}.sql"],
            target=dbt_target,
            profile=DBT_CONFIG["profile"],
            upload_dbt_project=True,
            dag=dag,
        )


def create_data_quality_check_callable(schema: str, uri_fn):
    """
    Factory function để tạo data quality check callable
    
    Args:
        schema: Schema name (vd: 'src_jira', 'dwh', 'jira')
        uri_fn: Function to get database URI
    """
    def data_quality_check(tgt_table: str, **kwargs) -> None:
        # Gọi function để lấy URI tại runtime
        uri = uri_fn() if callable(uri_fn) else uri_fn
        
        # Tạo full table name
        if schema:
            full_table_name = f"{schema}.{tgt_table}"
        else:
            full_table_name = tgt_table
        
        target_data = extract_sql_data(uri, f"SELECT * FROM {full_table_name} LIMIT 1000")
        
        
        suite_name = f"{schema}_{tgt_table}" if schema else tgt_table
        result = validate_dataframe(df=target_data, suite_name=suite_name)
        # Push kết quả validation lên XCom để notify sau
        kwargs['ti'].xcom_push(
            key=f'{suite_name}_validation_results',
            value=result
        )

        
        return None
    
    return data_quality_check


def create_data_notification_callable(schema: str, uri_fn, task_group_prefix: str):
    """
    Factory function để tạo data notification callable
    
    Args:
        schema: Schema name
        uri_fn: Function to get database URI
        task_group_prefix: Prefix của task group (vd: 'jira_to_staging', 'staging_to_warehouse')
    """
    def data_notification(src: str, tgt_table: str, **kwargs) -> None:
        # Gọi function để lấy URI tại runtime
        uri = uri_fn() if callable(uri_fn) else uri_fn
        
        suite_name = f"{schema}_{tgt_table}" if schema else tgt_table
        inner_group_id = f'{src}_to_{tgt_table}'
        quality_check_task_id = f'{schema}_{tgt_table}_quality_check'
        full_task_id_to_pull = f'{task_group_prefix}.{inner_group_id}.{quality_check_task_id}'
        print(f"Attempting to pull from task ID: {full_task_id_to_pull} , {suite_name}_validation_results")
        print(f"Pulling validation results from task ID: {full_task_id_to_pull}")
        # Pull validation result
        result = kwargs['ti'].xcom_pull(
            task_ids=full_task_id_to_pull, 
            key=f'{suite_name}_validation_results'
        )
        
        # Get row counts
        if schema:
            full_table_name = f"{schema}.{tgt_table}"
        else:
            full_table_name = tgt_table
            
        total_rows = extract_sql_data(uri, f"SELECT count(*) as total_rows FROM {full_table_name}")['total_rows'][0]
        prev_rows = int(Variable.get(f"{suite_name}_prev_rows", 0))
        new_rows_inserted = total_rows - prev_rows
        
        # Get SLACK_CONFIG tại runtime (lazy import)
        from config import get_slack_config
        slack_config = get_slack_config()
        
        # Send notification
        send_validation_results(
            table_name=suite_name,
            validation_result=result,
            slack_channel_id=slack_config['chat_id'],
            slack_bot_token=slack_config['bot_token'],
            total_rows=total_rows,
            new_rows_inserted=new_rows_inserted
        )
        
        Variable.set(f"{suite_name}_prev_rows", str(total_rows))
        
        return None
    
    return data_notification


def create_save_job_logs_callable(source_db: str, target_db: str):
    """
    Factory function để tạo save job logs callable
    
    Args:
        source_db: Source database name
        target_db: Target database name
    """
    def save_logs(**kwargs):
        import json
        from config import DB_URIS
        from utils.monitoring import save_job_log
        print(f"Saving job log for {kwargs.get('src')} to {kwargs.get('tgt_table')}")
        # Airflow auto-injects these into kwargs
        dag_id = kwargs['dag'].dag_id
        task_id = kwargs['task'].task_id
        execution_date = (
            kwargs.get('logical_date') or 
            kwargs.get('data_interval_start') or
            kwargs.get('execution_date')
        )
        if execution_date is None:
            execution_date = datetime.utcnow()
        ti = kwargs['ti']
        
        log_data = {
            'job_name': f"to_{target_db}",
            'source_db': source_db,
            'target_db': target_db,
            'source_table': json.dumps(kwargs.get('src')),
            'target_table': (kwargs.get('tgt_table', '')),
            'dag_id': dag_id,
            'task_id': task_id,
            'execution_time': execution_date,
            'status': kwargs.get('status', 'SUCCESS'),
        }
        
        log_uri = DB_URIS['monitoring']()
        job_id = save_job_log(log_uri, log_data)
        print(f"Saved job log with ID: {job_id}")
        # Push job_id for metrics task
        ti.xcom_push(key='job_id', value=job_id)
        
        return job_id
    
    return save_logs

def create_save_metrics_callable(schema: str, task_group_prefix: str):
    """
    Create callable for saving metrics
    Support cả Ingestion và DBT tasks
    """
    def save_metrics_task(**kwargs):
        from config import DB_URIS, DBT_CONFIG
        from utils.monitoring import save_metrics
        import os
        import json
        from sqlalchemy import create_engine, text
        from datetime import datetime, timezone
        
        ti = kwargs['ti']
        
        # ----------- LẤY job_id -----------
        tgt_table = kwargs.get('tgt_table')
        source = kwargs.get('src')
        source_type = kwargs.get('src_type')  # 'ingestion' hoặc 'dbt'
        
        log_task_id = f'success_save_logs_{schema}_{tgt_table}'
        group_id = f'{source}_to_{tgt_table}'
        
        if task_group_prefix and group_id:
            full_log_task_id = f"{task_group_prefix}.{group_id}.{log_task_id}"
        else:
            full_log_task_id = log_task_id
        
        job_id = ti.xcom_pull(task_ids=full_log_task_id, key='job_id')
        if not job_id:
            print(f"Warning: No job_id found from {full_log_task_id}")
            return
        
        
        # ================= DBT TASK =================
        if source_type == 'dbt':
            dbt_project_dir = DBT_CONFIG['project_dir']
            target_name = DBT_CONFIG['target_name']
            run_results_path = os.path.join(
                dbt_project_dir, 'target', target_name, 'run_results.json'
            )
            
            if not os.path.exists(run_results_path):
                print(f"Warning: run_results.json not found")
                return
            
            with open(run_results_path) as f:
                run_results = json.load(f)
                        
            execution_time = 0
            for r in run_results.get('results', []):
                if r['unique_id'].endswith(tgt_table):
                    execution_time = r.get('execution_time', 0)
                    break
            
            # ----------- QUERY DATA METRIC -----------
            engine = create_engine(DB_URIS['dwh']())
            with engine.connect() as conn:
                row_count_sql = text(
                    f"SELECT COUNT(*) AS cnt FROM {schema}.{tgt_table}"
                )
                row_count = conn.execute(row_count_sql).scalar()
                
                max_ts_sql = text(
                    f"""
                    SELECT MAX(etl_datetime)
                    FROM {schema}.{tgt_table}
                    """
                )
                max_updated_at = conn.execute(max_ts_sql).scalar()
            
            data_delay_minutes = None
            print(f"Max updated at: {max_updated_at}")
            if max_updated_at:
                max_updated_at_naive = max_updated_at.replace(tzinfo=None)
                now = datetime.utcnow()
                data_delay_minutes = int(
                    (now - max_updated_at_naive).total_seconds() / 60
                )
            
            metrics = {
                'total_duration': execution_time,
                'target_row_count': row_count,
                'max_updated_at': max_updated_at,
                'data_delay_minutes': data_delay_minutes,
                'source_row_count': None,
                'extract_duration': None,
                'load_duration': None,
                'peak_memory_mb': None
            }
        
        # ================= INGESTION TASK =================
        else:
            if task_group_prefix and group_id:
                log_task_id = f'success_save_logs_{schema}_{tgt_table}'
                full_extract_task_id = f"{task_group_prefix}.{group_id}.{log_task_id}"
            else:
                full_extract_task_id = log_task_id
            
            metrics = ti.xcom_pull(
                task_ids=full_extract_task_id,
                key='metrics'
            )
            
            if not metrics:
                print(f"Warning: No metrics found from {full_extract_task_id}")
                return
        
        # ----------- SAVE METRICS -----------
        log_uri = DB_URIS['monitoring']()
        save_metrics(log_uri, job_id, metrics)
    
    return save_metrics_task


def sync_fdw_tables(tgt_schema: str, src_schema: str, server_name: str, db_uri_fn) -> None:
    """
    Sync FDW tables - dùng chung cho tất cả pipelines
    
    Args:
        tgt_schema: Target schema name
        src_schema: Source schema name
        server_name: FDW server name
        db_uri_fn: Function to get database URI
    """
    # Gọi function để lấy URI tại runtime
    db_uri = db_uri_fn() if callable(db_uri_fn) else db_uri_fn
    pg_engine = create_engine(db_uri)
    
    with pg_engine.connect() as conn:
        conn.execute(text(f"CALL public.sync_fdw_tables('{tgt_schema}', '{src_schema}', '{server_name}');commit;"))
    
    return None