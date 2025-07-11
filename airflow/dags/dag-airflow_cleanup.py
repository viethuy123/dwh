from datetime import timedelta
from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule
 
default_args = {
    'owner': 'huynnx',
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
    'email_on_retry': False,
}

with DAG(
        dag_id='dag-cleanup_airflow_logs',
        default_args=default_args,
        schedule_interval='@daily',  
        catchup=False,               
) as dag:
    
    start = EmptyOperator(task_id='start', dag=dag)

    end = EmptyOperator(task_id='end', dag=dag, trigger_rule=TriggerRule.ALL_DONE)

    clean_scheduler_logs = BashOperator(
        task_id='clean_scheduler_logs',
        bash_command="""
        echo "Cleaning scheduler logs older than 7 days..."
        find /opt/airflow/logs/scheduler -type f -mtime +7 -print -delete
        """,
    )

    clean_dag_logs = BashOperator(
        task_id='clean_dag_logs',
        bash_command="""
        BASE_LOG_FOLDER="/opt/airflow/logs"
        MAX_LOG_AGE_IN_DAYS=30
        echo "Cleaning DAG logs in $BASE_LOG_FOLDER older than $MAX_LOG_AGE_IN_DAYS days..."
        find $BASE_LOG_FOLDER -type f -name '*.log' -mtime +$MAX_LOG_AGE_IN_DAYS -print -delete
        find $BASE_LOG_FOLDER -type d -empty -print -delete
        """,
    )

    start >> clean_dag_logs >> clean_scheduler_logs >> end
