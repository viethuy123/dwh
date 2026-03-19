# factories/dbt_factory.py
"""
Factory để tạo DBT transformation tasks - LAZY IMPORT VERSION
"""
from airflow.sdk import TaskGroup
from airflow.providers.standard.operators.python import PythonOperator
SKIP_QC_TABLES = ['dim_date','fct_member_monthly_snapshot']  # Danh sách các bảng sẽ skip data quality check

def _get_table_mapping(mapping_var: str) -> dict:

    from utils.mappings import (
        intermediate_mapping,
        report_mapping,
        dim_mapping,
        fct_mapping,
        snapshot_mapping,
        bridge_mapping
    )
    
    mappings = {
        'intermediate_mapping': intermediate_mapping,
        'report_mapping': report_mapping,
        'dim_mapping': dim_mapping,
        'fct_mapping': fct_mapping,
        'snapshot_mapping': snapshot_mapping,
        'bridge_mapping': bridge_mapping
    }
    return mappings.get(mapping_var, {})


def create_dbt_transformation_task_group(dag, source: str, pipeline_config: dict) -> tuple:

    source_db = pipeline_config['source_db']
    target_db = pipeline_config['target_db']
    target_schema = pipeline_config.get('tgt_schema', 'public') 
    dbt_target = pipeline_config['dbt_target']
    models_path = pipeline_config['models_path']
    
    table_mapping = _get_table_mapping(pipeline_config['table_mapping_var'])
    from config import DB_URIS, DBT_CONFIG
    from utils.common_tasks import (
        create_data_quality_check_callable,
        create_data_notification_callable,
        create_save_job_logs_callable,
        create_save_metrics_callable,
        _create_dbt_operator
    )
    
    # Xác định DB URI
    target_db_uri = DB_URIS['dwh']
    
    # Tạo callable functions - REUSE cho tất cả tables
    data_quality_fn = create_data_quality_check_callable(target_schema, target_db_uri)
    
    # Task group prefix
    # if target_schema == 'intermediates':
    #     task_group_prefix = 'staging_to_warehouse'
    # else:
    #     task_group_prefix = 'warehouse_to_mart'
    
    task_group_prefix = f'{source}_to_{target_schema}'
    
    data_notification_fn = create_data_notification_callable(
        target_schema,
        target_db_uri,
        task_group_prefix
    )
    save_logs_fn = create_save_job_logs_callable(source_db, target_db)
    save_metrics_fn = create_save_metrics_callable(target_schema, task_group_prefix)
    
    with TaskGroup(group_id=task_group_prefix, dag=dag) as outer_group:
        
        # Tạo inner task group cho từng table
        for tgt_table, src_table in table_mapping.items():
            src_table = f'{source}_{tgt_table}' if source else src_table
            
            with TaskGroup(group_id=f'{source}_to_{tgt_table}', dag=dag) as inner_group:
                
                # DBT Run task
                dbt_task = _create_dbt_operator(
                    task_id=f"dbt_{target_schema}_{tgt_table}",
                    mapping_var=pipeline_config["table_mapping_var"],
                    models_path=models_path,
                    target_schema=target_schema,
                    tgt_table=tgt_table,
                    dag=dag,
                    dbt_target=dbt_target,
                    DBT_CONFIG=DBT_CONFIG,
                )
                
                # Success logs
                success_logs = PythonOperator(
                    task_id=f'success_save_logs_{target_schema}_{tgt_table}',
                    python_callable=save_logs_fn,
                    op_kwargs={
                        'src': source,
                        'tgt_table': tgt_table,
                        'status': 'SUCCESS'
                    },
                    dag=dag,
                )
                save_metrics_task = PythonOperator(
                    task_id=f'metrics_{target_schema}_{tgt_table}',
                    python_callable=save_metrics_fn,
                    op_kwargs={
                        'src': source,
                        'tgt_table': tgt_table,
                        'src_type': 'dbt',
                    },
                    dag=dag,
                )
                
                # Data quality check
                quality_check = PythonOperator(
                    task_id=f'{target_schema}_{tgt_table}_quality_check',
                    python_callable=data_quality_fn,
                    op_kwargs={'tgt_table': tgt_table},
                    dag=dag,
                )
                
                # Data notification
                notification = PythonOperator(
                    task_id=f'{target_schema}_{tgt_table}_notification',
                    python_callable=data_notification_fn,
                    op_kwargs={
                        'src': source,
                        'tgt_table': tgt_table
                    },
                    dag=dag,
                )
                
                # Failure logs
                failure_logs = PythonOperator(
                    task_id=f'failure_save_logs_{target_schema}_{tgt_table}',
                    python_callable=save_logs_fn,
                    op_kwargs={
                        'src': source,
                        'tgt_table': tgt_table,
                        'status': 'FAILURE'
                    },
                    trigger_rule='all_failed',
                    dag=dag,
                )
                
                # Dependencies
                if tgt_table in SKIP_QC_TABLES:
                    dbt_task >> success_logs
                    
                else:
                    dbt_task >> success_logs >> save_metrics_task >> quality_check >> notification
                    
                dbt_task >> failure_logs


    return outer_group