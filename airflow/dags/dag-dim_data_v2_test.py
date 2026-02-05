# dags/dag-dim-data-v2-test.py

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python    import PythonOperator
from datetime import datetime

from utils.dbt_result_parser import (
    collect_dbt_results,
    notify_failed_models,
)

with DAG(
    dag_id="dag_dim_data_v2_test",
    start_date=datetime(2026, 2, 1),
    schedule=None,        # TEST MANUAL
    catchup=False,
    tags=["dbt", "dim", "v2"],
) as dag:

    dbt_run_dim = BashOperator(
    task_id="dbt_run_dim",
    bash_command="""
        cd /opt/airflow/dbt &&
        dbt build \
        --select tag:dim \
        --target dwh
        """,
    )

    collect_results = PythonOperator(
        task_id="collect_dbt_results",
        python_callable=collect_dbt_results,
    )

    notify = PythonOperator(
        task_id="notify_failed_models",
        python_callable=notify_failed_models,
    )

    dbt_run_dim >> collect_results >> notify
