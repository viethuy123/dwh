from airflow.sdk import DAG, Variable, TaskGroup
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from datetime import datetime, timedelta
from utils.extract_data import extract_sql_data
from utils.data_quality import validate_dataframe
from utils.data_quality_notification import send_validation_results
from utils.etl_job_logs import save_etl_job_logs
from sqlalchemy import create_engine, text


default_args = {
    'owner': 'huynnx',
    'start_date': datetime.today() - timedelta(days=1),
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
    'depends_on_past': False,
}

dag = DAG(
    dag_id='dag-jisseki_to_staging',
    default_args=default_args,
    schedule='@once',
    catchup=False
)

start = EmptyOperator(task_id='start', dag=dag)

end = EmptyOperator(task_id='end', dag=dag, trigger_rule='all_done')

slack_bot_token = Variable.get('slack-bot_token')
slack_chat_id = Variable.get('slack-chat_id')

pg_user = Variable.get("pg_user")
pg_pwd = Variable.get("pg_password")
pg_host = Variable.get("pg_host")
pg_port = Variable.get("pg_port")
pg_uri = "postgresql+psycopg2://{}:{}@{}:{}/{}".format(pg_user, pg_pwd, pg_host, pg_port, "stg")  
wh_uri = "postgresql+psycopg2://{}:{}@{}:{}/{}".format(pg_user, pg_pwd, pg_host, pg_port, "dwh") 
monitor_uri = "postgresql+psycopg2://{}:{}@{}:{}/{}".format(pg_user, pg_pwd, pg_host, pg_port, "monitoring")  

jisseki_user = Variable.get("jisseki_user")
jisseki_pwd = Variable.get("jisseki_password")    
jisseki_host = Variable.get("jisseki_host")
jisseki_port = Variable.get("jisseki_port")
jisseki_uri = "mysql+pymysql://{}:{}@{}:{}/{}".format(jisseki_user, jisseki_pwd, jisseki_host, jisseki_port, "go_jisseki")

src_tables = ['projects','project_customer']
tgt_tables = [f"stg_{table}" for table in src_tables]

def extract_load_jisseki_data(src_table:str, tgt_table:str) -> None:

    df = extract_sql_data(jisseki_uri, f"SELECT * FROM {src_table}")

    df['etl_datetime'] = datetime.now()

    pg_engine = create_engine(pg_uri)
    
    with pg_engine.connect() as conn: # type: ignore
        result = conn.execute(
        text(
            f"""SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = 'src_jisseki' 
            AND table_name = '{tgt_table}'
            )"""
        )
    )
        table_exists = result.scalar()
        if table_exists:
            conn.execute(text(f"TRUNCATE TABLE src_jisseki.{tgt_table};commit;"))
            df.to_sql(tgt_table, con=conn, if_exists='append', index=False, chunksize=5000,schema='src_jisseki')
        else:
            df.to_sql(tgt_table, con=conn, if_exists='replace', index=False, chunksize=5000,schema='src_jisseki')

    return None


def data_quality_check(tgt_table:str,**kwargs) -> None:

    target_data = extract_sql_data(pg_uri, f"SELECT * FROM src_jisseki.{tgt_table}")
 
    result = validate_dataframe(df=target_data, suite_name=f"src_jisseki-{tgt_table}")

    kwargs['ti'].xcom_push(
        key=f'src_jisseki-{tgt_table}_validation_results',
        value=result
    )

    return None

def data_notification(src_table:str, tgt_table:str, **kwargs) -> None:

    result = kwargs['ti'].xcom_pull(
        task_ids=f'jisseki_to_staging.{src_table}_to_{tgt_table}.src_jisseki-{tgt_table}_quality_check', 
        key=f'src_jisseki-{tgt_table}_validation_results')
    total_rows = extract_sql_data(pg_uri, f"SELECT count(*) as total_rows FROM src_jisseki.{tgt_table}")['total_rows'][0]
    prev_rows = int(Variable.get(f"src_jisseki-{tgt_table}_prev_rows",0))
    new_rows_inserted = total_rows - prev_rows
    send_validation_results(
        table_name=f'src_jisseki-{tgt_table}', 
        validation_result=result, 
        slack_channel_id=slack_chat_id, 
        slack_bot_token=slack_bot_token,
        total_rows=total_rows, 
        new_rows_inserted=new_rows_inserted)
    Variable.set(f"src_jisseki-{tgt_table}_prev_rows",total_rows)

    return None


def save_job_logs(src_table:str | list, tgt_table:str, status:str, **context) -> None:

    execution_time = context['execution_date']

    save_etl_job_logs(
        sql_uri=monitor_uri,
        log_table='etl_job_logs',
        job_name=context['task'].task_group.group_id if context['task'].task_group else 'No TaskGroup',
        source_db='jisseki',
        target_db='staging',
        source_table=[src_table] if isinstance(src_table, str) else src_table,
        target_table= tgt_table,
        dag_id= context['dag'].dag_id,
        task_id= context['task'].task_id,
        execution_time=getattr(execution_time, '__wrapped__', execution_time),
        status=status
    )

    return None

def sync_fdw_tables(tgt_schema:str, src_schema:str, server_name:str) -> None:

    pg_engine = create_engine(wh_uri)

    with pg_engine.connect() as conn: # type: ignore
        conn.execute(text(f"CALL public.sync_fdw_tables('{tgt_schema}', '{src_schema}', '{server_name}');commit;"))

    return None


with TaskGroup(group_id='jisseki_to_staging', dag=dag) as outer_group:

    for src_table, tgt_table in zip(src_tables,tgt_tables):

        with TaskGroup(group_id = f'{src_table}_to_{tgt_table}', dag=dag) as inner_group:

            extract_load_task = PythonOperator(
                    dag=dag,
                    task_id=f'extract_load_src_jisseki-{tgt_table}',
                    python_callable=extract_load_jisseki_data,
                    op_kwargs ={
                        'src_table':src_table,
                        'tgt_table':tgt_table
                    }
                )


            data_quality_task = PythonOperator(
                    dag=dag,
                    task_id=f'src_jisseki-{tgt_table}_quality_check',
                    python_callable=data_quality_check,
                    op_kwargs={
                        'tgt_table':tgt_table
                    }
                )

            

            data_notification_task = PythonOperator(
                    dag=dag,
                    task_id=f'src_jisseki-{tgt_table}_notification',
                    python_callable=data_notification,
                    op_kwargs={
                        'tgt_table':tgt_table,
                        'src_table':src_table
                    }
                )


            success_save_logs_task = PythonOperator(
                    dag=dag,
                    task_id=f'success_save_logs_src_jisseki-{tgt_table}',
                    python_callable=save_job_logs,
                    op_kwargs={
                        'src_table':src_table,
                        'tgt_table':tgt_table,
                        'status':'SUCCESS'
                    }
                )


            failure_save_logs_task = PythonOperator(
                    dag=dag,
                    task_id=f'failure_save_logs_src_jisseki-{tgt_table}',
                    python_callable=save_job_logs,
                    op_kwargs={
                        'src_table':src_table,
                        'tgt_table':tgt_table,
                        'status':'FAILURE'	
                    },
                    trigger_rule='all_failed'
                )
            
            extract_load_task >> success_save_logs_task >> data_quality_task >> data_notification_task 
            extract_load_task >> failure_save_logs_task


sync_fdw_tables_task = PythonOperator(
    dag=dag,
    task_id='sync_fdw_tables-src_jisseki_to_jisseki_fdw',
    python_callable=sync_fdw_tables,
    op_kwargs={
        'tgt_schema':'jisseki_fdw',
        'src_schema':'src_jisseki',
        'server_name':'staging_server'
        },
    trigger_rule='all_done'
    )

start >> outer_group >> sync_fdw_tables_task >> end
            