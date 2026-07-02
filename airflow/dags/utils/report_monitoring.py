from airflow.decorators import task
from airflow.utils.trigger_rule import TriggerRule


@task
def save_success_log(report_name: str):

    from airflow.sdk import get_current_context
    from utils.monitoring import save_job_log
    from config import DB_URIS
    from datetime import datetime

    context = get_current_context()

    return save_job_log(
        DB_URIS["monitoring"](),
        {
            "job_name": report_name,
            "source_db": "dbt",
            "target_db": "dwh",
            "source_table": None,
            "target_table": report_name,
            "status": "SUCCESS",
            "execution_time": datetime.utcnow(),
            "dag_id": context["dag"].dag_id,
            "task_id": context["task"].task_id,
        },
    )


@task
def save_metrics(job_id: int, report_name: str):

    from config import DB_URIS
    from utils.monitoring import save_metrics as _save_metrics

    import json
    import os

    run_results_path = "/opt/airflow/dbt/target/run_results.json"

    total_duration = 0

    if os.path.exists(run_results_path):

        with open(run_results_path) as f:
            run_results = json.load(f)

        total_duration = sum(
            r.get("execution_time", 0)
            for r in run_results.get("results", [])
            if report_name in r.get("unique_id", "")
        )

    metrics = {
        "total_duration": total_duration,
        "target_row_count": None,
        "max_updated_at": None,
        "data_delay_minutes": None,
        "source_row_count": None,
        "extract_duration": None,
        "load_duration": None,
        "peak_memory_mb": None,
    }

    if job_id is None:
        print(f"Skipping metrics save for {report_name}: job_id is None")
        return None

    _save_metrics(
        DB_URIS["monitoring"](),
        job_id,
        metrics,
    )


@task(trigger_rule=TriggerRule.ONE_FAILED)
def save_failure_log(report_name: str):

    from airflow.sdk import get_current_context
    from utils.monitoring import save_job_log
    from config import DB_URIS
    from datetime import datetime

    context = get_current_context()

    save_job_log(
        DB_URIS["monitoring"](),
        {
            "job_name": report_name,
            "source_db": "dbt",
            "target_db": "dwh",
            "source_table": None,
            "target_table": report_name,
            "status": "FAILURE",
            "execution_time": datetime.utcnow(),
            "dag_id": context["dag"].dag_id,
            "task_id": context["task"].task_id,
        },
    )


@task(trigger_rule=TriggerRule.ONE_FAILED)
def failure_alert(report_name: str):

    from airflow.operators.python import get_current_context

    from config import get_slack_config

    from utils.data_quality_notification import (
        send_failure_notification,
    )

    context = get_current_context()

    slack = get_slack_config()

    send_failure_notification(
        report_name=report_name,
        slack_bot_token=slack["bot_token"],
        slack_channel_id=slack["chat_id"],
        error_message=str(context.get("exception", "")),
    )
