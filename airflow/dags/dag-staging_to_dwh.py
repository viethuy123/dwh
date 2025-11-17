from airflow.sdk import Variable, DAG, TaskGroup
from airflow_dbt_python.operators.dbt import DbtRunOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from utils.data_quality import validate_dataframe
from utils.data_quality_notification import send_validation_results
from utils.etl_job_logs import save_etl_job_logs
from utils.extract_data import extract_sql_data
from utils.mappings import warehouse_mapping
from datetime import timedelta, datetime
from sqlalchemy import create_engine, text


default_args = {
    "owner": "huynnx",
    "depends_on_past": False,
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
    'depends_on_past': False,
}

dag = DAG(
    dag_id="dag-staging_to_dwh",
    default_args=default_args,
    schedule='30 3 * * *',
    start_date=datetime.today() - timedelta(days=1),
    catchup=False,
    dagrun_timeout=timedelta(minutes=60),
)

start = EmptyOperator(task_id='start', dag=dag)

end = EmptyOperator(task_id='end', dag=dag, trigger_rule='all_done')

slack_bot_token = Variable.get('slack-bot_token')
slack_chat_id = Variable.get('slack-chat_id')

pg_user = Variable.get("pg_user")
pg_pwd = Variable.get("pg_password")
pg_host = Variable.get("pg_host")
pg_port = Variable.get("pg_port")
pg_uri = "postgresql+psycopg2://{}:{}@{}:{}/{}".format(pg_user, pg_pwd, pg_host, pg_port, "dwh")
mart_uri = "postgresql+psycopg2://{}:{}@{}:{}/{}".format(pg_user, pg_pwd, pg_host, pg_port, "dtm")
monitor_uri = "postgresql+psycopg2://{}:{}@{}:{}/{}".format(pg_user, pg_pwd, pg_host, pg_port, "monitoring")  

def data_quality_check(tgt_table:str,**kwargs) -> None:

    target_data = extract_sql_data(pg_uri, f"SELECT * FROM {tgt_table} LIMIT 1000")
 
    result = validate_dataframe(df=target_data, suite_name=f"dwh-{tgt_table}")

    kwargs['ti'].xcom_push(
        key=f'dwh-{tgt_table}_validation_results',
        value=result
    )

    return None

def data_notification(tgt_table:str, **kwargs) -> None:

    result = kwargs['ti'].xcom_pull(
        task_ids=f'staging_to_warehouse.dwh_{tgt_table}.dwh-{tgt_table}_quality_check', 
        key=f'dwh-{tgt_table}_validation_results')
    total_rows = extract_sql_data(pg_uri, f"SELECT count(*) as total_rows FROM {tgt_table}")['total_rows'][0]
    prev_rows = int(Variable.get(f"dwh-{tgt_table}_prev_rows",0))
    new_rows_inserted = total_rows - prev_rows
    send_validation_results(
        table_name=f'dwh-{tgt_table}', 
        validation_result=result, 
        slack_channel_id=slack_chat_id, 
        slack_bot_token=slack_bot_token,
        total_rows=total_rows, 
        new_rows_inserted=new_rows_inserted)
    Variable.set(f"dwh-{tgt_table}_prev_rows",str(total_rows))

    return None


def save_job_logs(src_table:str | list, tgt_table:str, status:str, **context) -> None:

    execution_time = context['logical_date']

    save_etl_job_logs(
        sql_uri=monitor_uri,
        log_table='etl_job_logs',
        job_name=context['task'].task_group.group_id if context['task'].task_group else 'No TaskGroup',
        source_db='stg',
        target_db='dwh',
        source_table=[src_table] if isinstance(src_table, str) else src_table,
        target_table=tgt_table,
        dag_id= context['dag'].dag_id,
        task_id= context['task'].task_id,
        execution_time=getattr(execution_time, '__wrapped__', execution_time),
        status=status
    )

    return None

def sync_fdw_tables(tgt_schema:str, src_schema:str, server_name:str) -> None:

    pg_engine = create_engine(mart_uri)

    with pg_engine.connect() as conn: # type: ignore
        conn.execute(text(f"CALL public.sync_fdw_tables('{tgt_schema}', '{src_schema}', '{server_name}');commit;"))

    return None

with TaskGroup(group_id='staging_to_warehouse', dag=dag) as outer_group:

    for tgt_table, src_table in warehouse_mapping.items():

        with TaskGroup(group_id = f'dwh_{tgt_table}', dag=dag) as inner_group:

            dbt_run_dwh = DbtRunOperator(
                dag=dag,
                task_id=f"dbt_dwh_{tgt_table}",
                project_dir = "/opt/airflow/dbt", 
                profiles_dir = "/opt/airflow/dbt/.dbt/", 
                select = [f"path:models/dwh/{tgt_table}.sql"],
                target = "dwh",  
                profile = "dwh_project",
                upload_dbt_project=True
            )


            data_quality_task = PythonOperator(
                    dag=dag,
                    task_id=f'dwh-{tgt_table}_quality_check',
                    python_callable=data_quality_check,
                    op_kwargs={
                        'tgt_table':tgt_table
                    }
                )

            

            data_notification_task = PythonOperator(
                    dag=dag,
                    task_id=f'dwh-{tgt_table}_notification',
                    python_callable=data_notification,
                    op_kwargs={
                        'tgt_table':tgt_table
                    }
                )


            success_save_logs_task = PythonOperator(
                    dag=dag,
                    task_id=f'success_save_logs_dwh-{tgt_table}',
                    python_callable=save_job_logs,
                    op_kwargs={
                        'src_table':src_table,
                        'tgt_table':tgt_table,
                        'status':'SUCCESS'
                    }
                )


            failure_save_logs_task = PythonOperator(
                    dag=dag,
                    task_id=f'failure_save_logs_dwh-{tgt_table}',
                    python_callable=save_job_logs,
                    op_kwargs={
                        'src_table':src_table,
                        'tgt_table':tgt_table,
                        'status':'FAILURE'	
                    },
                    trigger_rule='all_failed'
                )
            
            dbt_run_dwh >> success_save_logs_task >> data_quality_task >> data_notification_task 
            dbt_run_dwh >> failure_save_logs_task

sync_fdw_tables_task = PythonOperator(
    dag=dag,
    task_id='sync_fdw_tables-dwh_public_to_dwh_fdw',
    python_callable=sync_fdw_tables,
    op_kwargs={
        'tgt_schema':'dwh_fdw',
        'src_schema':'public',
        'server_name':'warehouse_server'
        },
    trigger_rule='all_done'
    )

start >> outer_group >> sync_fdw_tables_task >> end