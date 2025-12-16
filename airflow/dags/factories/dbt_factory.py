# factories/dbt_factory.py
"""
Factory để tạo DBT transformation tasks
"""
from airflow.sdk import TaskGroup
from airflow.providers.standard.operators.python import PythonOperator
from airflow_dbt_python.operators.dbt import DbtRunOperator
from utils.common_tasks import (
    create_data_quality_check_callable,
    create_data_notification_callable,
    create_save_job_logs_callable,
)
from utils.mappings import \
warehouse_mapping, jira_dtm_mapping, report_mapping , dim_mapping , fct_mapping


def _get_table_mapping(mapping_var: str) -> dict:
    """
    Lấy table mapping từ utils.mappings
    
    Args:
        mapping_var: Tên biến mapping ('warehouse_mapping' hoặc 'jira_dtm_mapping')
    """
    mappings = {
        'warehouse_mapping': warehouse_mapping,
        'report_mapping': report_mapping,
        'dim_mapping': dim_mapping,
        'fct_mapping': fct_mapping,
    }
    return mappings.get(mapping_var, {})


def create_dbt_transformation_task_group(dag, source_schema, pipeline_config: dict) -> tuple:
    """
    Tạo TaskGroup cho DBT transformation tasks
    
    Args:
        dag: DAG object
        pipeline_config: Config từ DBT_PIPELINES trong pipeline_config.py
    
    Returns:
        tuple: (outer_group, sync_fdw_task hoặc None)
    """
    source_db = pipeline_config['source_db']
    target_db = pipeline_config['target_db']
    target_schema = pipeline_config.get('tgt_schema', 'public') 
    dbt_target = pipeline_config['dbt_target']
    models_path = pipeline_config['models_path']
    
    # Lấy table mapping
    table_mapping = _get_table_mapping(pipeline_config['table_mapping_var'])
    
    # Import DB_URIS và DBT_CONFIG tại runtime
    from config import DB_URIS, DBT_CONFIG
    
    # Xác định DB URI để query
    target_db_uri = DB_URIS['staging']
    
    # Tạo callable functions
    data_quality_fn = create_data_quality_check_callable(target_schema, target_db_uri)
    
    # Task group prefix tùy theo pipeline
    if target_schema == 'intermediates':  # dwh
        task_group_prefix = 'staging_to_warehouse'
    else:  # dtm
        task_group_prefix = 'warehouse_to_mart'
    
    data_notification_fn = create_data_notification_callable(
        target_schema,
        target_db_uri,
        task_group_prefix
    )
    save_logs_fn = create_save_job_logs_callable(source_db, target_db)
    
    with TaskGroup(group_id=task_group_prefix, dag=dag) as outer_group:
        
        # Tạo inner task group cho từng table
        for tgt_table, src_table in table_mapping.items():
            src_table = f'{source_schema}_{tgt_table}' if source_schema else src_table
            with TaskGroup(group_id=f'{src_table}_{tgt_table}', dag=dag) as inner_group:
                
                # DBT Run task
                dbt_run = DbtRunOperator(
                    task_id=f"dbt_{tgt_table}",
                    project_dir=DBT_CONFIG['project_dir'],
                    profiles_dir=DBT_CONFIG['profiles_dir'],
                    select=[f"path:{models_path}/{target_schema}/{tgt_table}.sql"],
                    target=dbt_target,
                    profile=DBT_CONFIG['profile'],
                    upload_dbt_project=True,
                    dag=dag,
                )
                
                # Success logs
                success_logs = PythonOperator(
                    task_id=f'success_save_logs-{tgt_table}',
                    python_callable=save_logs_fn,
                    op_kwargs={
                        'src_table': src_table,
                        'tgt_table': tgt_table,
                        'status': 'SUCCESS'
                    },
                    dag=dag,
                )
                
                # Data quality check
                quality_check = PythonOperator(
                    task_id=f'{target_schema}-{tgt_table}_quality_check',
                    python_callable=data_quality_fn,
                    op_kwargs={'tgt_table': tgt_table},
                    dag=dag,
                )
                
                # Data notification
                notification = PythonOperator(
                    task_id=f'{target_schema}-{tgt_table}_notification',
                    python_callable=data_notification_fn,
                    op_kwargs={
                        'src_table': src_table,
                        'tgt_table': tgt_table

                    },
                    dag=dag,
                )
                
                # Failure logs
                failure_logs = PythonOperator(
                    task_id=f'failure_save_logs-{tgt_table}',
                    python_callable=save_logs_fn,
                    op_kwargs={
                        'src_table': src_table,
                        'tgt_table': tgt_table,
                        'status': 'FAILURE'
                    },
                    trigger_rule='all_failed',
                    dag=dag,
                )
                
                # Dependencies
                dbt_run >> success_logs >> quality_check >> notification
                dbt_run >> failure_logs

    return outer_group