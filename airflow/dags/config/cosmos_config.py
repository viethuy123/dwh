"""dbt / Cosmos shared configuration."""

DBT_PROJECT_DIR = "/opt/airflow/dbt"
DBT_PROFILES_DIR = "/opt/airflow/dbt"
DBT_EXECUTABLE_PATH = "/home/airflow/.local/bin/dbt"


REPORTS = [
    "hr_data_user_new",
    "hr_data_user_snapshot",
    "detect_resources",
    "hr_attendance",
    "hr_skill_members",
]