from datetime import timedelta
from airflow.sdk import DAG
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
    dag_id='dag-cleanup_airflow_logs',
    default_args=default_args,
    schedule='@daily',  
    catchup=False,               
) as dag:
    
    start = EmptyOperator(task_id='start', dag=dag)

    end = EmptyOperator(task_id='end', dag=dag, trigger_rule='all_done')

    clean_scheduler_logs = BashOperator(
        task_id='clean_scheduler_logs',
        bash_command="""
        echo "Cleaning scheduler logs older than 3 days..."
        find /opt/airflow/logs/scheduler -type f -mtime +3 -print -delete
        """,
    )

    clean_dag_logs = BashOperator(
        task_id='clean_dag_logs',
        bash_command="""
        BASE_LOG_FOLDER="/opt/airflow/logs"
        MAX_LOG_AGE_IN_DAYS=7
        echo "Cleaning DAG logs in $BASE_LOG_FOLDER older than $MAX_LOG_AGE_IN_DAYS days..."
        find $BASE_LOG_FOLDER -type f -name '*.log' -mtime +$MAX_LOG_AGE_IN_DAYS -print -delete
        find $BASE_LOG_FOLDER -type d -empty -print -delete
        """,
    )

    start >> clean_dag_logs >> clean_scheduler_logs >> end
