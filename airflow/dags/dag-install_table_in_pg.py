from datetime import timedelta, datetime
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

# Cấu hình kết nối PostgreSQL
# LƯU Ý: Nếu PostgreSQL service tên là dwh_postgres
POSTGRES_HOST = "dwh_postgres"
POSTGRES_USER = "dev_user"
POSTGRES_PASSWORD = "dev_password" # Tốt nhất nên dùng Secret hoặc Environment Variable

# Đường dẫn của file script trong Airflow container
SQL_SCRIPT_PATH = "/opt/airflow/sql/create_fdw.pgsql"

default_args = {
    'owner': 'huynnx',
    'start_date': datetime.today() - timedelta(days=1),
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
    'depends_on_past': False,
}

with DAG(
    dag_id='fdw_sync_and_setup',
    default_args=default_args,
    schedule='@once', # Chạy một lần để thiết lập hoặc @hourly/@daily để sync
    catchup=False,
) as dag:
    
    start = EmptyOperator(task_id='start')

    # Task để chạy file .pgsql sử dụng BashOperator và lệnh psql
    # -h: host
    # -U: user
    # -d: database ban đầu để kết nối
    # -f: file script
    setup_fdw_procedure = BashOperator(
        task_id='setup_fdw_procedure',
        # Đặt biến môi trường PGPASSWORD để psql không hỏi mật khẩu
        bash_command=f"""
        echo "Running FDW setup and sync procedure..."
        PGPASSWORD={POSTGRES_PASSWORD} psql -h {POSTGRES_HOST} -U {POSTGRES_USER} -d postgres -f {SQL_SCRIPT_PATH}
        """,
    )

    # Task ví dụ để gọi stored procedure bạn vừa tạo
    # LƯU Ý: Task này cần chạy SAU khi setup_fdw_procedure chạy thành công
    sync_staging_tables = BashOperator(
        task_id='sync_staging_tables',
        bash_command=f"""
        echo "Calling sync_fdw_tables for staging data..."
        PGPASSWORD={POSTGRES_PASSWORD} psql -h {POSTGRES_HOST} -U {POSTGRES_USER} -d dtm -c "CALL sync_fdw_tables('stg', 'public', 'staging_server');"
        """,
    )

    end = EmptyOperator(task_id='end', trigger_rule='all_done')

    start >> setup_fdw_procedure >> sync_staging_tables >> end