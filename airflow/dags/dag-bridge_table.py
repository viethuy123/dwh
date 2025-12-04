# dags/dag_jisseki_facts.py
from airflow.sdk import Variable, DAG, TaskGroup
from airflow_dbt_python.operators.dbt import DbtRunOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sensors.external_task import ExternalTaskSensor
from utils.data_quality import validate_dataframe
from utils.data_quality_notification import send_validation_results
from utils.etl_job_logs import save_etl_job_logs
from utils.extract_data import extract_sql_data
from utils.mappings import bridge_dtm_mapping
from datetime import timedelta, datetime


default_args = {
    "owner": "huynnx",
    "depends_on_past": False,
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    dag_id="dag-bridge_table",
    default_args=default_args,
    schedule='0 4 * * *',  # 4 AM - 1 tiếng sau dimensions
    start_date=datetime.today() - timedelta(days=1),
    catchup=False,
    dagrun_timeout=timedelta(minutes=45),
    tags=['bridge', 'layer-3']
)

start = EmptyOperator(task_id='start', dag=dag)
end = EmptyOperator(task_id='end', dag=dag, trigger_rule='all_done')

# Sensor: Đợi facts DAG hoàn thành
wait_for_facts = ExternalTaskSensor(
    task_id='wait_for_facts',
    external_dag_id='facts',
    external_task_id='end',
    allowed_states=['success'],
    failed_states=['failed', 'skipped'],
    mode='reschedule',  # Giải phóng worker slot khi đang đợi
    poke_interval=60,  # Check mỗi 60s
    timeout=3600,  # Timeout sau 1 tiếng
    execution_delta=timedelta(hours=1),  # Facts chạy trước 1 tiếng
    dag=dag
)

# Variables (giống dimensions)
slack_bot_token = Variable.get('slack-bot_token')
slack_chat_id = Variable.get('slack-chat_id')
pg_user = Variable.get("pg_user")
pg_pwd = Variable.get("pg_password")
pg_host = Variable.get("pg_host")
pg_port = Variable.get("pg_port")
pg_uri = "postgresql+psycopg2://{}:{}@{}:{}/{}".format(pg_user, pg_pwd, pg_host, pg_port, "dtm")  
monitor_uri = "postgresql+psycopg2://{}:{}@{}:{}/{}".format(pg_user, pg_pwd, pg_host, pg_port, "monitoring")  


def data_notification(tgt_table: str, **kwargs) -> None:
    result = kwargs['ti'].xcom_pull(
        task_ids=f'bridge_group.jisseki_dtm_{tgt_table}.jisseki_dtm-{tgt_table}_quality_check', 
        key=f'bridge_dtm-{tgt_table}_validation_results'
    )
    total_rows = extract_sql_data(pg_uri, f"SELECT count(*) as total_rows FROM dtm.bridge.{tgt_table}")['total_rows'][0]
    prev_rows = int(Variable.get(f"bridge_dtm-{tgt_table}_prev_rows", 0))
    new_rows_inserted = total_rows - prev_rows
    
    send_validation_results(
        table_name=f'bridge_dtm-{tgt_table}', 
        validation_result=result, 
        slack_channel_id=slack_chat_id, 
        slack_bot_token=slack_bot_token,
        total_rows=total_rows, 
        new_rows_inserted=new_rows_inserted
    )
    Variable.set(f"bridge_dtm-{tgt_table}_prev_rows", str(total_rows))
    return None


def save_job_logs(src_table: str | list, tgt_table: str, status: str, **context) -> None:
    execution_time = context['logical_date']
    save_etl_job_logs(
        sql_uri=monitor_uri,
        log_table='etl_job_logs',
        job_name=context['task'].task_group.group_id if context['task'].task_group else 'No TaskGroup',
        source_db='dwh',
        target_db='dtm',
        source_table=[src_table] if isinstance(src_table, str) else src_table,
        target_table=f"dtm.bridge.{tgt_table}",
        dag_id=context['dag'].dag_id,
        task_id=context['task'].task_id,
        execution_time=getattr(execution_time, '__wrapped__', execution_time),
        status=status
    )
    return None


with TaskGroup(group_id='bridge_group', dag=dag) as bridge_group:
    
    for tgt_table, src_table in bridge_dtm_mapping.items():
        
        with TaskGroup(group_id=f'bridge_dtm_{tgt_table}', dag=dag) as inner_group:
            
            dbt_run_dtm = DbtRunOperator(
                dag=dag,
                task_id=f"dbt_bridge_dtm_{tgt_table}",
                project_dir="/opt/airflow/dbt", 
                profiles_dir="/opt/airflow/dbt/.dbt/", 
                select=[f"path:models/dtm/bridge/{tgt_table}.sql"],  # Đổi path
                target="dtm",  
                profile="dwh_project",
                upload_dbt_project=True
            )
            
            
            data_notification_task = PythonOperator(
                dag=dag,
                task_id=f'bridge_dtm-{tgt_table}_notification',
                python_callable=data_notification,
                op_kwargs={'tgt_table': tgt_table}
            )
            
            success_save_logs_task = PythonOperator(
                dag=dag,
                task_id=f'success_save_logs_bridge_dtm-{tgt_table}',
                python_callable=save_job_logs,
                op_kwargs={
                    'src_table': src_table,
                    'tgt_table': tgt_table,
                    'status': 'SUCCESS'
                }
            )
            
            failure_save_logs_task = PythonOperator(
                dag=dag,
                task_id=f'failure_save_logs_bridge_dtm-{tgt_table}',
                python_callable=save_job_logs,
                op_kwargs={
                    'src_table': src_table,
                    'tgt_table': tgt_table,
                    'status': 'FAILURE'	
                },
                trigger_rule='one_failed'
            )
            
            dbt_run_dtm >> success_save_logs_task  >> data_notification_task 
            dbt_run_dtm >> failure_save_logs_task

# Flow: start → đợi dimensions → chạy facts → end
start >> wait_for_facts >> bridge_group >> end