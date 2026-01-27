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
        
        suite_name = f"{schema}-{tgt_table}" if schema else tgt_table
        result = validate_dataframe(df=target_data, suite_name=suite_name)
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
    def data_notification(src_table: str, tgt_table: str, **kwargs) -> None:
        # Gọi function để lấy URI tại runtime
        uri = uri_fn() if callable(uri_fn) else uri_fn
        
        suite_name = f"{schema}-{tgt_table}" if schema else tgt_table
        inner_group_id = f'{src_table}_{tgt_table}'
        quality_check_task_id = f'{schema}-{tgt_table}_quality_check'
        full_task_id_to_pull = f'{task_group_prefix}.{inner_group_id}.{quality_check_task_id}'
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
    def save_job_logs(src_table: str | list, tgt_table: str, status: str, **context) -> None:
        execution_time = (
            context.get('logical_date') or 
            context.get('data_interval_start') or 
            context.get('execution_date')
        )
        
        # Nếu vẫn không có (dataset trigger case), dùng current time
        if execution_time is None:
            execution_time = datetime.utcnow()
        
        # Get monitoring URI tại runtime (lazy import)
        from config import DB_URIS
        monitoring_uri = DB_URIS['monitoring']()
        
        save_etl_job_logs(
            sql_uri=monitoring_uri,
            log_table='etl_job_logs',
            job_name=context['task'].task_group.group_id if context['task'].task_group else 'No TaskGroup',
            source_db=source_db,
            target_db=target_db,
            source_table=[src_table] if isinstance(src_table, str) else src_table,
            target_table=tgt_table,
            dag_id=context['dag'].dag_id,
            task_id=context['task'].task_id,
            execution_time=getattr(execution_time, '__wrapped__', execution_time),
            status=status
        )
        
        return None
    
    return save_job_logs


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