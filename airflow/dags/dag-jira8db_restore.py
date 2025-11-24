from airflow.sdk import Variable, DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from utils.dropbox_actions import generate_dropbox_access_token, file_exists, download_backup_file
from datetime import timedelta, datetime

yesterday_str = (datetime.now()-timedelta(days=1)).strftime("%Y%m%d")

default_args = {
    "owner": "huynnx",
    "depends_on_past": False,
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
    'depends_on_past': False
}

dag = DAG(
    dag_id="dag-jira8db_restore",
    default_args=default_args,
    schedule='0 19 * * *',
    start_date=datetime.today() - timedelta(days=1),
    catchup=False,
    dagrun_timeout=timedelta(minutes=60),
)

def push_dropbox_access_token(**kwargs):
    
    access_token = generate_dropbox_access_token(
        app_key=Variable.get("DROPBOX_APP_KEY"),
        app_secret=Variable.get("DROPBOX_APP_SECRET"),
        refresh_token=Variable.get("DROPBOX_REFRESH_TOKEN")
    )
    
    kwargs['ti'].xcom_push(key='dropbox_access_token', value=access_token)
    
    return True

def download_dropbox_file(**kwargs):
    
    access_token = kwargs['ti'].xcom_pull(task_ids='push_dropbox_access_token', key='dropbox_access_token')
    
    file_exist, file_path = file_exists(
        filename=f"jira8db_bk_{yesterday_str}",
        access_token=access_token,
    )
    
    if file_exist:
        download_backup_file(
            file_path=file_path,
            download_path=f"/opt/airflow/database_backup/jira8db_bk_{yesterday_str}_22.zip",
            access_token=access_token
        )
    else:
        print("File not found.")
        
    return True
    
with dag:
    
    start = EmptyOperator(task_id='start', dag=dag)

    end = EmptyOperator(task_id='end', dag=dag, trigger_rule='all_done')
    
    push_dropbox_access_token_task = PythonOperator(
        task_id='push_dropbox_access_token',
        python_callable=push_dropbox_access_token,
        dag=dag,
    )
    
    download_dropbox_file_task = PythonOperator(
        task_id='download_dropbox_file',
        python_callable=download_dropbox_file,
        dag=dag,
    )
    
    unzip_file_task = BashOperator(
        task_id='unzip_file',
        bash_command="""
            echo "Unzipping jira backups and deleting previous backups..."
            python3 /opt/airflow/py/unzip_backup_file.py
        """,
        dag=dag,
    )
    
    restore_backup_db = BashOperator(
        task_id='restore_jira8db',
        bash_command="""
            echo "Restoring database..."
            /opt/airflow/scripts/restore_jira8db.sh
        """,
        env={"MYSQL_PWD": Variable.get("jira_password")},
        dag=dag,
    )
    
    start >> push_dropbox_access_token_task >> download_dropbox_file_task >> unzip_file_task >> restore_backup_db >> end