from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from datetime import datetime
from sqlalchemy import create_engine, text

# --- 1. CODE PROCEDURE MỚI (SQL) ---
# Đây là code Procedure mới chứa logic DROP/IMPORT/DELETE để đồng bộ cột và bảng bị xoá.
NEW_PROCEDURE_SQL = """
CREATE OR REPLACE PROCEDURE sync_fdw_tables(
    local_schema TEXT,
    remote_schema TEXT,
    server_name TEXT,
    metadata_schema TEXT DEFAULT 'fdw_metadata'
)
LANGUAGE plpgsql
AS $$
DECLARE
    foreign_table TEXT;
    remote_tables TEXT[]; 
BEGIN
    
    EXECUTE format(
        'SELECT array_agg(table_name) FROM %I.tables WHERE table_schema = %L AND table_type = %L',
        metadata_schema, remote_schema, 'BASE TABLE'
    ) INTO remote_tables;

    IF remote_tables IS NOT NULL THEN
        FOREACH foreign_table IN ARRAY remote_tables
        LOOP
            RAISE NOTICE 'Syncing table: %', foreign_table;
            EXECUTE format('DROP FOREIGN TABLE IF EXISTS %I.%I CASCADE', local_schema, foreign_table); 
            EXECUTE format($f$
                IMPORT FOREIGN SCHEMA %I
                LIMIT TO (%I)
                FROM SERVER %I
                INTO %I;
            $f$, remote_schema, foreign_table, server_name, local_schema);
            
        END LOOP;
    END IF;

    FOR foreign_table IN
        SELECT ft.relname
        FROM pg_foreign_table f
        JOIN pg_class ft ON f.ftrelid = ft.oid
        JOIN pg_namespace ns ON ft.relnamespace = ns.oid
        WHERE ns.nspname = local_schema
    LOOP
        IF NOT (foreign_table = ANY(remote_tables)) THEN
            RAISE NOTICE 'Removing orphaned table: %', foreign_table;
            EXECUTE format('DROP FOREIGN TABLE IF EXISTS %I.%I CASCADE', local_schema, foreign_table);
        END IF;
    END LOOP;

END;
$$;
"""

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 1), 
    'retries': 0,
}

dag = DAG(
    dag_id='dag-deploy_fdw_procedure',
    default_args=default_args,
    schedule='@once',
    catchup=False
)

def deploy_procedure_logic():
    pg_user = Variable.get("pg_user")
    pg_pwd = Variable.get("pg_password")
    pg_host = Variable.get("pg_host")
    pg_port = Variable.get("pg_port")
    db_name = "dtm" 
    
    wh_uri = f"postgresql+psycopg2://{pg_user}:{pg_pwd}@{pg_host}:{pg_port}/{db_name}"
    engine = create_engine(wh_uri)
    
    with engine.connect() as conn:
        with conn.begin(): # Bắt đầu giao dịch
            print("Procedure to PostgreSQL...")
            # CREATE OR REPLACE PROCEDURE
            conn.execute(text(NEW_PROCEDURE_SQL))
        print("ok")

# --- 4. TASK DEPLOY ---
deploy_task = PythonOperator(
    task_id='deploy_fdw_sync_procedure',
    python_callable=deploy_procedure_logic,
    dag=dag
)