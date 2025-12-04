# dags/dag_jisseki_dimensions.py
from airflow.sdk import Variable, DAG, TaskGroup
from airflow_dbt_python.operators.dbt import DbtRunOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from utils.mappings import snapshot_dtm_mapping
from datetime import timedelta, datetime


default_args = {
    "owner": "huynnx",
    "depends_on_past": False,
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    dag_id="snap",
    default_args=default_args,
    schedule='0 2 * * *',  # 2 AM - Tự chạy
    start_date=datetime.today() - timedelta(days=1),
    catchup=False,
    dagrun_timeout=timedelta(minutes=30),
    tags=['snap']
)

start = EmptyOperator(task_id='start', dag=dag)
end = EmptyOperator(task_id='end', dag=dag, trigger_rule='all_done')

# Variables
slack_bot_token = Variable.get('slack-bot_token')
slack_chat_id = Variable.get('slack-chat_id')
pg_user = Variable.get("pg_user")
pg_pwd = Variable.get("pg_password")
pg_host = Variable.get("pg_host")
pg_port = Variable.get("pg_port")
pg_uri = "postgresql+psycopg2://{}:{}@{}:{}/{}".format(pg_user, pg_pwd, pg_host, pg_port, "dtm")  
monitor_uri = "postgresql+psycopg2://{}:{}@{}:{}/{}".format(pg_user, pg_pwd, pg_host, pg_port, "monitoring")  

with TaskGroup(group_id='snapshot_group', dag=dag) as snap_group:
    
    for tgt_table, src_table in snapshot_dtm_mapping.items():
        
        with TaskGroup(group_id=f'dtm_dim_{tgt_table}', dag=dag) as inner_group:
            
            dbt_run_dtm = DbtRunOperator(
                dag=dag,
                task_id=f"dbt_dtm_{tgt_table}",
                project_dir="/opt/airflow/dbt", 
                profiles_dir="/opt/airflow/dbt/.dbt/", 
                select=[f"path:models/dtm/snapshots_dwh/{tgt_table}.sql"],
                target="dtm",  
                profile="dwh_project",
                upload_dbt_project=True
            )


            dbt_run_dtm

start >> snap_group >> end