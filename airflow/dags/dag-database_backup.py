from datetime import timedelta
from airflow.sdk import DAG, Variable
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from datetime import datetime, timedelta

 
default_args = {
    'owner': 'huynnx',
    'start_date': datetime.today() - timedelta(days=1),
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
    'depends_on_past': False,
}

with DAG(
        dag_id='dag-database_backup',
        default_args=default_args,
        schedule='59 16 * * *',  
        catchup=False,               
) as dag:
    
    start = EmptyOperator(task_id='start', dag=dag)

    end = EmptyOperator(task_id='end', dag=dag, trigger_rule='all_done')

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
    
    s3_delete = BashOperator(
        task_id='s3_delete',
        bash_command="""
        echo "Deleting old backups from S3..."
        python3 /opt/airflow/py/prev_backup_delete.py
        """,
    )

    start >> [warehouse_backup, metabase_backup] >> s3_upload >> s3_delete >>end
