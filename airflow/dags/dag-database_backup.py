from datetime import timedelta
from airflow.models import DAG, Variable
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule
 
default_args = {
    'owner': 'huynnx',
    'start_date': days_ago(1),
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
    'email_on_failure': False,
    'email_on_retry': False,
}

with DAG(
        dag_id='dag-database_backup',
        default_args=default_args,
        schedule='@daily',  
        catchup=False,               
) as dag:
    
    start = EmptyOperator(task_id='start', dag=dag)

    end = EmptyOperator(task_id='end', dag=dag, trigger_rule=TriggerRule.ALL_DONE)

    warehouse_backup = BashOperator(
        task_id='warehouse_backup',
        bash_command="""
        echo "Backing up warehouse databases..."
        /opt/airflow/scripts/warehouse_backup.sh
        """,
        env={"PGPASSWORD": Variable.get("pg_password")}
    )

    metabase_backup = BashOperator(
        task_id='metabase_backup',
        bash_command="""
        echo "Backing up metabase database..."
        /opt/airflow/scripts/metabase_backup.sh
        """,
        env={"PGPASSWORD": Variable.get("mb_pg_password")}
    )

    s3_upload = BashOperator(
        task_id='s3_upload',
        bash_command="""
        echo "Uploading backups to S3..."
        python3 /opt/airflow/py/backup_upload.py
        """,
    )


    start >> [warehouse_backup, metabase_backup] >> s3_upload >> end
