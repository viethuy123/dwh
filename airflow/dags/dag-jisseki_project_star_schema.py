from airflow.models.dag import DAG
from airflow_dbt_python.operators.dbt import DbtRunOperator, DbtSnapshotOperator
from airflow.operators.empty import EmptyOperator
from datetime import timedelta, datetime

DBT_PROJECT_DIR = "/opt/airflow/dbt"
DBT_PROFILES_DIR = "/opt/airflow/dbt/.dbt/"
DBT_PROFILE_NAME = "dwh_project"
DBT_TARGET = "dtm"

default_args = {
    "owner": "huynnx",
    "depends_on_past": False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id="dag-dwh-star-schema-build",
    default_args=default_args,
    schedule='30 01 * * *', 
    start_date=datetime(2025, 12, 1),
    catchup=False,
    dagrun_timeout=timedelta(minutes=120),
    tags=['dwh', 'star_schema', 'dbt'],
) as dag:
    
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')

    dbt_snapshot = DbtSnapshotOperator(
        task_id='dbt_run_snapshot_customer',
        project_dir=DBT_PROJECT_DIR,
        profiles_dir=DBT_PROFILES_DIR,
        profile=DBT_PROFILE_NAME,
        target=DBT_TARGET,
        select=['tag:scd2'],
    )

    dbt_dim = DbtRunOperator(
        task_id='dbt_run_dimensions',
        project_dir=DBT_PROJECT_DIR,
        profiles_dir=DBT_PROFILES_DIR,
        profile=DBT_PROFILE_NAME,
        target=DBT_TARGET,
        select=['path:models/dtm/dimension/'], 
    )

    dbt_bridge = DbtRunOperator(
        task_id='dbt_run_bridges',
        project_dir=DBT_PROJECT_DIR,
        profiles_dir=DBT_PROFILES_DIR,
        profile=DBT_PROFILE_NAME,
        target=DBT_TARGET,
        select=['path:models/dtm/bridge/'],
    )

    dbt_fact = DbtRunOperator(
        task_id='dbt_run_facts',
        project_dir=DBT_PROJECT_DIR,
        profiles_dir=DBT_PROFILES_DIR,
        profile=DBT_PROFILE_NAME,
        target=DBT_TARGET,
        select=['path:models/dtm/fact/'],
    )

    start >> dbt_snapshot >> dbt_dim >>  dbt_fact >> dbt_bridge >> end