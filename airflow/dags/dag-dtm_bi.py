from airflow.sdk import DAG, TaskGroup
from airflow_dbt_python.operators.dbt import DbtRunOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from datetime import timedelta, datetime
from utils.mappings import bi_dtm

default_args = {
    "owner": "huynnx",
    "depends_on_past": False,
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
    'depends_on_past': False,
}

dag = DAG(
    dag_id="dag-bi_dtm",
    default_args=default_args,
    schedule='0 21 * * *',
    start_date=datetime.today() - timedelta(days=1),
    catchup=False,
    dagrun_timeout=timedelta(minutes=60),
)

start = EmptyOperator(task_id='start', dag=dag)

end = EmptyOperator(task_id='end', dag=dag, trigger_rule='all_done')

with TaskGroup(group_id='bi_mart', dag=dag) as outer_group:

    for mview in bi_dtm:

        with TaskGroup(group_id = f'bi_mart_{mview}', dag=dag) as inner_group:

            dbt_run_mview = DbtRunOperator(
                dag=dag,
                task_id=f"dbt_bi_mart_{mview}",
                project_dir = "/opt/airflow/dbt", 
                profiles_dir = "/opt/airflow/dbt/.dbt/", 
                select = [f"path:models/dtm/bi/{mview}.sql"],
                target = "dtm",  
                profile = "dwh_project",
                upload_dbt_project=True
            )

start >> outer_group >> end