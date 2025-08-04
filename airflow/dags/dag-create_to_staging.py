from airflow.sdk import DAG, Variable, TaskGroup
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from datetime import datetime, timedelta
from utils.extract_data import extract_mongo_data, extract_sql_data
from utils.data_quality import validate_dataframe
from utils.data_quality_notification import send_validation_results
from utils.etl_job_logs import save_etl_job_logs
from sqlalchemy import create_engine, text
from bson import ObjectId
import json


default_args = {
    'owner': 'huynnx',
    'start_date': datetime.today() - timedelta(days=1),
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
    'depends_on_past': False,
}

dag = DAG(
    dag_id='dag-create_to_staging',
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

create_user = Variable.get("create_user")
create_pwd = Variable.get("create_password")    
create_host = Variable.get("create_host")
create_port = Variable.get("create_port")
create_uri = "mongodb://{}:{}@{}:{}/{}".format(create_user, create_pwd, create_host, create_port, "portal")

src_tables = ['projects','project_categories', 'pods',
              'project_members','user_skills', 'billable_efforts_approveds',
              'users','branches','user_positions',
              'company_departments','user_infos',
              'profit_loss_expenses','profit_loss_project_expenses',
              'staff_attendances', 'staff_attendance_types','salaries']
tgt_tables = [f"stg_{table}" for table in src_tables]


def convert_object_ids_recursive(val):
    if isinstance(val, ObjectId):
        return str(val)
    if isinstance(val, dict):
        return {k: convert_object_ids_recursive(v) for k,v in val.items()}
    if isinstance(val, list):
        return [convert_object_ids_recursive(i) for i in val]
    return val

def serialize_complex_types(val):
    if isinstance(val, dict) or isinstance(val, list):
        return json.dumps(val)
    return val

def convert_all(val):
    val = convert_object_ids_recursive(val)
    val = serialize_complex_types(val)
    if isinstance(val, str):
        val = val.replace('\x00', '')
    return val

def extract_load_create_data(src_table:str, tgt_table:str) -> None:

    df = extract_mongo_data(create_uri,'portal',src_table)

    df = df.map(convert_all)

    df['etl_datetime'] = datetime.now()

    pg_engine = create_engine(pg_uri)
    
    with pg_engine.connect() as conn: # type: ignore
        result = conn.execute(
        text(
            f"""SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = 'src_create' 
            AND table_name = '{tgt_table}'
            )"""
        )
    )
        table_exists = result.scalar()
        if table_exists:
            conn.execute(text(f"TRUNCATE TABLE src_create.{tgt_table};commit;"))
            df.to_sql(tgt_table, con=conn, if_exists='append', index=False, chunksize=5000,schema='src_create')
        else:
            df.to_sql(tgt_table, con=conn, if_exists='replace', index=False, chunksize=5000,schema='src_create')

    return None


def data_quality_check(tgt_table:str,**kwargs) -> None:

    target_data = extract_sql_data(pg_uri, f"SELECT * FROM src_create.{tgt_table} limit 100")
 
    result = validate_dataframe(df=target_data, suite_name=f"src_create-{tgt_table}")

    kwargs['ti'].xcom_push(
        key=f'src_create-{tgt_table}_validation_results',
        value=result
    )

    return None

def data_notification(src_table:str, tgt_table:str, **kwargs) -> None:

    result = kwargs['ti'].xcom_pull(
        task_ids=f'create_to_staging.{src_table}_to_{tgt_table}.src_create-{tgt_table}_quality_check', 
        key=f'src_create-{tgt_table}_validation_results'
    )
    total_rows = extract_sql_data(pg_uri, f"SELECT count(*) as total_rows FROM src_create.{tgt_table}")['total_rows'][0]
    prev_rows = int(Variable.get(f"src_create-{tgt_table}_prev_rows",0))
    new_rows_inserted = total_rows - prev_rows
    send_validation_results(
        table_name=f'src_create-{tgt_table}', 
        validation_result=result, 
        slack_channel_id=slack_chat_id, 
        slack_bot_token=slack_bot_token,
        total_rows=total_rows, 
        new_rows_inserted=new_rows_inserted)
    Variable.set(f"src_create-{tgt_table}_prev_rows",str(total_rows))

    return None


def save_job_logs(src_table:str | list, tgt_table:str, status:str, **context) -> None:

    execution_time = context['logical_date']

    save_etl_job_logs(
        sql_uri=monitor_uri,
        log_table='etl_job_logs',
        job_name=context['task'].task_group.group_id if context['task'].task_group else 'No TaskGroup',
        source_db='create',
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


with TaskGroup(group_id='create_to_staging', dag=dag) as outer_group:

    for src_table, tgt_table in zip(src_tables,tgt_tables):

        with TaskGroup(group_id = f'{src_table}_to_{tgt_table}', dag=dag) as inner_group:

            extract_load_task = PythonOperator(
                    dag=dag,
                    task_id=f'extract_load_src_create-{tgt_table}',
                    python_callable=extract_load_create_data,
                    op_kwargs ={
                        'src_table':src_table,
                        'tgt_table':tgt_table
                    }
                )


            data_quality_task = PythonOperator(
                    dag=dag,
                    task_id=f'src_create-{tgt_table}_quality_check',
                    python_callable=data_quality_check,
                    op_kwargs={
                        'tgt_table':tgt_table
                    }
                )

            

            data_notification_task = PythonOperator(
                    dag=dag,
                    task_id=f'src_create-{tgt_table}_notification',
                    python_callable=data_notification,
                    op_kwargs={
                        'tgt_table':tgt_table,
                        'src_table':src_table
                    }
                )


            success_save_logs_task = PythonOperator(
                    dag=dag,
                    task_id=f'success_save_logs_src_create-{tgt_table}',
                    python_callable=save_job_logs,
                    op_kwargs={
                        'src_table':src_table,
                        'tgt_table':tgt_table,
                        'status':'SUCCESS'
                    }
                )


            failure_save_logs_task = PythonOperator(
                    dag=dag,
                    task_id=f'failure_save_logs_src_create-{tgt_table}',
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
    task_id='sync_fdw_tables-src_create_to_create_fdw',
    python_callable=sync_fdw_tables,
    op_kwargs={
        'tgt_schema':'create_fdw',
        'src_schema':'src_create',
        'server_name':'staging_server'
        },
    trigger_rule='all_done'
    )
       

start >> outer_group >> sync_fdw_tables_task >> end
            