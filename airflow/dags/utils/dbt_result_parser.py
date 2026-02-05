# utils/dbt_result_parser.py

import json
from pathlib import Path
from datetime import datetime

DBT_TARGET_PATH = Path("/opt/airflow/dbt/target/dwh/run_results.json")


def collect_dbt_results():
    if not DBT_TARGET_PATH.exists():
        raise FileNotFoundError("run_results.json not found")

    with open(DBT_TARGET_PATH) as f:
        results = json.load(f)["results"]

    for r in results:
        model = r["unique_id"].split(".")[-1]
        status = r["status"].upper()
        message = r.get("message")

        rows = None
        if r.get("adapter_response"):
            rows = r["adapter_response"].get("rows_affected")

        started_at = r["timing"][0]["started_at"]
        finished_at = r["timing"][-1]["completed_at"]
        

        # === MOCK SAVE LOG ===
        print(f"""
            MODEL: {model}
            STATUS: {status}
            ROWS: {rows}
            ERROR: {message}
            START: {started_at}
            END: {finished_at}
            """)

        # 👉 Sau này map thẳng sang:
        # save_job_log(...)
        # save_metrics(...)


def notify_failed_models():
    with open(DBT_TARGET_PATH) as f:
        results = json.load(f)["results"]

    failed = [
        r for r in results
        if r["status"] in ("error", "fail")
    ]

    if not failed:
        print("✅ No failed dbt models")
        return

    for r in failed:
        model = r["unique_id"].split(".")[-1]
        message = r.get("message")

        print(f"""
            ❌ DBT MODEL FAILED
            Model: {model}
            Error: {message}
            """)
