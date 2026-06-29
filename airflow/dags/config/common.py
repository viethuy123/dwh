"""Common DAG settings and helper functions"""
from datetime import timedelta
import pendulum
TIMEZONE_NAME = 'Asia/Ho_Chi_Minh'
TIMEZONE = pendulum.timezone(TIMEZONE_NAME)


def get_local_now():
    return pendulum.now(TIMEZONE)


def to_local_datetime(value):
    if value is None:
        return None

    if value.tzinfo is None:
        return pendulum.instance(value, tz=TIMEZONE)

    return pendulum.instance(value).in_timezone(TIMEZONE)


DEFAULT_ARGS = {
    'owner': 'huy',
    'start_date': pendulum.datetime(2026, 1, 1, tz=TIMEZONE),
    'retries': 5,
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
