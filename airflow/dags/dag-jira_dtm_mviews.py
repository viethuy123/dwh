from airflow.sdk import DAG, TaskGroup
from airflow_dbt_python.operators.dbt import DbtRunOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from datetime import timedelta, datetime
from utils.mappings import jira_mviews

default_args = {
    "owner": "huynnx",
    "depends_on_past": False,
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
    'depends_on_past': False,
}

dag = DAG(
    dag_id="dag-jira_dtm_mviews",
    default_args=default_args,
    schedule="@once",
    start_date=datetime.today() - timedelta(days=1),
    catchup=False,
    dagrun_timeout=timedelta(minutes=60),
)

start = EmptyOperator(task_id='start', dag=dag)

end = EmptyOperator(task_id='end', dag=dag, trigger_rule='all_done')

with TaskGroup(group_id='jira_mart_mviews', dag=dag) as outer_group:

    for mview in jira_mviews:

        with TaskGroup(group_id = f'jira_mart_{mview}', dag=dag) as inner_group:

            dbt_run_mview = DbtRunOperator(
                dag=dag,
                task_id=f"dbt_jira_mart_{mview}",
                project_dir = "/opt/airflow/dbt", 
                profiles_dir = "/opt/airflow/dbt/.dbt/", 
                select = [f"path:models/dtm/jira/{mview}.sql"],
                target = "dtm",  
                profile = "dwh_project",
                upload_dbt_project=True
            )

start >> outer_group >> end