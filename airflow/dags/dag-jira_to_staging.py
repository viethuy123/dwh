from airflow.sdk import DAG, Variable, TaskGroup
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from datetime import datetime, timedelta
from utils.extract_data import extract_sql_data
from utils.data_quality import validate_dataframe
from utils.data_quality_notification import send_validation_results
from utils.etl_job_logs import save_etl_job_logs
from sqlalchemy import create_engine, text
import pandas as pd
import gc # Import thư viện Garbage Collector

default_args = {
    'owner': 'huynnx',
    'start_date': datetime.today() - timedelta(days=1),
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
    'depends_on_past': False,
}

dag = DAG(
    dag_id='dag-jira_to_staging',
    default_args=default_args,
    schedule='0 20 * * *',
    max_active_runs=5,
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

jira_user = Variable.get("jira_user")
jira_pwd = Variable.get("jira_password")    
jira_host = Variable.get("jira_host")
jira_port = Variable.get("jira_port")
jira_uri = "mysql+pymysql://{}:{}@{}:{}/{}".format(jira_user, jira_pwd, jira_host, jira_port, "jira8db")

TABLE_CONFIGS = [
    {'name': 'project',       'type': 'light', 'chunksize': None}, # None = Load all
    {'name': 'issuestatus',   'type': 'light', 'chunksize': None},
    {'name': 'resolution',    'type': 'light', 'chunksize': None},
    {'name': 'priority',      'type': 'light', 'chunksize': None},
    {'name': 'issuetype',     'type': 'light', 'chunksize': None},
    {'name': 'customfieldoption', 'type': 'light', 'chunksize': None},
    {'name': 'app_user', 'type': 'light', 'chunksize': None},
    {'name': 'worklog',       'type': 'heavy', 'chunksize': 100000},
    {'name': 'jiraissue',     'type': 'heavy', 'chunksize': 100000},
    {'name': 'customfieldvalue', 'type': 'heavy', 'chunksize': 100000},
]


def convert_all(val):
    if isinstance(val, str):
        val = val.replace('\x00', '')
    return val

def extract_load_jira_data(src_table:str, tgt_table:str, chunk_size: int | None) -> None:
    src_engine = create_engine(jira_uri, pool_pre_ping=True)
    pg_engine = create_engine(pg_uri)

    # 1. Đảm bảo SCHEMA tồn tại
    with pg_engine.begin() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS src_jira"))

    print(f"Start loading {src_table}. Mode: {'Chunking ' + str(chunk_size) if chunk_size else 'Full Load'}")

    # ---  CHUNKING ---
    if chunk_size:
        is_first_chunk = True 
        
        with src_engine.connect().execution_options(stream_results=True) as src_conn:
            df_iterator = pd.read_sql(f"SELECT * FROM {src_table}", src_conn, chunksize=chunk_size)
            
            for i, df_chunk in enumerate(df_iterator):
                df_chunk = df_chunk.map(convert_all)
                df_chunk['etl_datetime'] = datetime.now()
                
                if is_first_chunk:
                    load_mode = 'replace' 
                    is_first_chunk = False 
                else:
                    load_mode = 'append' 
                
                with pg_engine.begin() as pg_conn:
                    df_chunk.to_sql(
                        tgt_table, 
                        con=pg_conn, 
                        if_exists=load_mode, 
                        index=False, 
                        schema='src_jira', 
                        method='multi', 
                        chunksize=50000
                    )
                
                print(f"Loaded chunk {i+1} ({len(df_chunk)} rows) into src_jira.{tgt_table}. Mode: {load_mode}")
                
                del df_chunk 
                gc.collect()
        
    # --- FULL LOAD ---
    else:
        df = pd.read_sql(f"SELECT * FROM {src_table}", src_engine)
        if not df.empty:
            df = df.map(convert_all)
            df['etl_datetime'] = datetime.now()
            with pg_engine.begin() as pg_conn:
                # Full Load dùng luôn 'replace'
                df.to_sql(tgt_table, con=pg_conn, if_exists='replace', index=False, 
                              schema='src_jira', method='multi')
            print(f"Loaded full table {src_table}: {len(df)} rows. Mode: replace")
        else:
            print(f"Source table {src_table} is empty. No data loaded.")

    return None
def data_quality_check(tgt_table:str,**kwargs) -> None:

    target_data = extract_sql_data(pg_uri, f"SELECT * FROM src_jira.{tgt_table} limit 1000")
 
    result = validate_dataframe(df=target_data, suite_name=f"src_jira-{tgt_table}")

    kwargs['ti'].xcom_push(
        key=f'src_jira-{tgt_table}_validation_results',
        value=result
    )

    return None

def data_notification(src_table:str, tgt_table:str, **kwargs) -> None:

    result = kwargs['ti'].xcom_pull(
        task_ids=f'jira_to_staging.{src_table}_to_{tgt_table}.src_jira-{tgt_table}_quality_check', 
        key=f'src_jira-{tgt_table}_validation_results')
    total_rows = extract_sql_data(pg_uri, f"SELECT count(*) as total_rows FROM src_jira.{tgt_table}")['total_rows'][0]
    prev_rows = int(Variable.get(f"src_jira-{tgt_table}_prev_rows",0))
    new_rows_inserted = total_rows - prev_rows
    send_validation_results(
        table_name=f'src_jira-{tgt_table}', 
        validation_result=result, 
        slack_channel_id=slack_chat_id, 
        slack_bot_token=slack_bot_token,
        total_rows=total_rows, 
        new_rows_inserted=new_rows_inserted)
    Variable.set(f"src_jira-{tgt_table}_prev_rows",str(total_rows))

    return None


def save_job_logs(src_table:str | list, tgt_table:str, status:str, **context) -> None:

    execution_time = context['logical_date']

    save_etl_job_logs(
        sql_uri=monitor_uri,
        log_table='etl_job_logs',
        job_name=context['task'].task_group.group_id if context['task'].task_group else 'No TaskGroup',
        source_db='jira',
        target_db='staging',
        source_table=[src_table] if isinstance(src_table, str) else src_table,
        target_table=tgt_table,
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


with TaskGroup(group_id='jira_to_staging', dag=dag) as outer_group:

    for config in TABLE_CONFIGS:
        src_table = config['name']
        tgt_table = f"stg_{src_table}"
        table_type = config['type']
        chunk_size = config['chunksize']
        assigned_pool = 'heavy_task_pool' if table_type == 'heavy' else 'default_pool'

        with TaskGroup(group_id = f'{src_table}_to_{tgt_table}', dag=dag) as inner_group:

            extract_load_task = PythonOperator(
                dag=dag,
                task_id=f'extract_load_src_jira-{tgt_table}',
                python_callable=extract_load_jira_data,
                op_kwargs={
                    'src_table': src_table,
                    'tgt_table': tgt_table,
                    'chunk_size': chunk_size
                },
                pool=assigned_pool, 
            )


            data_quality_task = PythonOperator(
                    dag=dag,
                    task_id=f'src_jira-{tgt_table}_quality_check',
                    python_callable=data_quality_check,
                    op_kwargs={
                        'tgt_table':tgt_table
                    }
                )

            

            data_notification_task = PythonOperator(
                    dag=dag,
                    task_id=f'src_jira-{tgt_table}_notification',
                    python_callable=data_notification,
                    op_kwargs={
                        'tgt_table':tgt_table,
                        'src_table':src_table
                    }
                )


            success_save_logs_task = PythonOperator(
                    dag=dag,
                    task_id=f'success_save_logs_src_jira-{tgt_table}',
                    python_callable=save_job_logs,
                    op_kwargs={
                        'src_table':src_table,
                        'tgt_table':tgt_table,
                        'status':'SUCCESS'
                    }
                )


            failure_save_logs_task = PythonOperator(
                    dag=dag,
                    task_id=f'failure_save_logs_src_jira-{tgt_table}',
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
    task_id='sync_fdw_tables-src_jira_to_jira_fdw',
    python_callable=sync_fdw_tables,
    op_kwargs={
        'tgt_schema':'jira_fdw',
        'src_schema':'src_jira',
        'server_name':'staging_server'
        },
    trigger_rule='all_done'
    )
       

start >> outer_group >> sync_fdw_tables_task >> end
            