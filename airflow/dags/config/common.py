"""Common DAG settings and helper functions"""
from datetime import datetime, timedelta
import pendulum
TIMEZONE = pendulum.timezone('Asia/Ho_Chi_Minh')
DEFAULT_ARGS = {
    'owner': 'huy',
    'start_date': pendulum.datetime(2026, 1, 1, tz=TIMEZONE),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'depends_on_past': False,
}
DEFAULT_CHECK_DAG = {
    'trigger_rule': 'none_failed',
}
def get_pool_name(table_type: str) -> str:
    """Map table type to Airflow pool"""
    return 'heavy_task_pool' if table_type == 'heavy' else 'default_pool'

def get_target_table_name(source_table: str, source_key: str) -> str:
    """Generate staging table name"""
    return f"stg_{source_key}_{source_table}"