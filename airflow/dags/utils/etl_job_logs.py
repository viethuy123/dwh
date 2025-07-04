from sqlalchemy import create_engine, text
from datetime import datetime
import json


def save_etl_job_logs(sql_uri: str, log_table: str, **kwargs) -> None:
    engine = create_engine(sql_uri)
    with engine.connect() as conn: # type: ignore
        conn.execute(text(
            f"""
            INSERT INTO 
                {log_table} (job_name, source_db, target_db, source_table, target_table, dag_id, task_id, execution_time, status, created_time) 
            VALUES (:job_name, :source_db, :target_db, :source_table, :target_table, :dag_id, :task_id, :execution_time, :status, :created_time);
            COMMIT;
            """
        ),{
            'job_name': kwargs.get('job_name'),
            'source_db': kwargs.get('source_db'),
            'target_db': kwargs.get('target_db'),
            'source_table': json.dumps(kwargs.get('source_table')) if isinstance(kwargs.get('source_table'), list) else kwargs.get('source_table'),
            'target_table': kwargs.get('target_table'),
            'dag_id': kwargs.get('dag_id'),
            'task_id': kwargs.get('task_id'),
            'execution_time': kwargs.get('execution_time'),
            'status': kwargs.get('status'),
            'created_time': datetime.now()
        })
