"""
ETL Monitoring utilities
"""
from datetime import datetime
from sqlalchemy import create_engine, text
import psutil
import os
import time


class ETLMonitor:
    """
    Lightweight monitor for ETL jobs
    Tracks timing, data volume, resources
    """
    
    def __init__(self):
        self.start_time = time.time()
        self.metrics = {}
        
        # Track memory
        try:
            process = psutil.Process(os.getpid())
            self.initial_memory_mb = process.memory_info().rss / 1024 / 1024
        except:
            self.initial_memory_mb = 0
    
    def record_extract(self, row_count: int):
        self.metrics['source_row_count'] = row_count
        self.metrics['extract_duration'] = time.time() - self.start_time
    
    def record_load(self, row_count: int):
        self.metrics['target_row_count'] = row_count
        self.metrics['load_duration'] = time.time() - self.metrics.get('extract_duration', 0) - self.start_time
    
    def finalize(self):
        self.metrics['total_duration'] = time.time() - self.start_time
        
        # Memory
        try:
            process = psutil.Process(os.getpid())
            self.metrics['peak_memory_mb'] = process.memory_info().rss / 1024 / 1024
        except:
            self.metrics['peak_memory_mb'] = 0
        
        return self.metrics


def save_job_log(db_uri: str, log_data: dict) -> int:
    """
    Save job execution log
    Returns: job_id
    """
    engine = create_engine(db_uri)
    
    create_sql = """
    CREATE TABLE IF NOT EXISTS etl_job_logs (
        id SERIAL PRIMARY KEY,
        job_name VARCHAR(255),
        source_db VARCHAR(100),
        target_db VARCHAR(100),
        source_table JSONB,
        target_table VARCHAR(255),
        dag_id VARCHAR(255),
        task_id VARCHAR(255),
        execution_time TIMESTAMP,
        status VARCHAR(50),
        created_at TIMESTAMP DEFAULT NOW()
    )
    """
    
    insert_sql = """
    INSERT INTO etl_job_logs (
        job_name, source_db, target_db, source_table, target_table,
        dag_id, task_id, execution_time, status
    ) VALUES (
        :job_name, :source_db, :target_db, :source_table, :target_table,
        :dag_id, :task_id, :execution_time, :status
    )
    RETURNING id
    """
    
    with engine.begin() as conn:
        conn.execute(text(create_sql))
        result = conn.execute(text(insert_sql), log_data)
        return result.scalar()


def save_metrics(db_uri: str, job_id: int, metrics: dict):
    """
    Save performance metrics
    """
    print(f"Saving metrics for job_id {job_id} to {db_uri}")
    engine = create_engine(db_uri)
    
    create_sql = """
    CREATE TABLE IF NOT EXISTS etl_metrics (
        id SERIAL PRIMARY KEY,
        job_id INTEGER,
        total_duration FLOAT,
        target_row_count BIGINT,
        max_updated_at TIMESTAMP,
        data_delay_minutes FLOAT,
        source_row_count BIGINT,
        extract_duration FLOAT,
        load_duration FLOAT,
        peak_memory_mb FLOAT,
        created_at TIMESTAMP DEFAULT NOW()
    )
    """
    
    insert_sql = """
    INSERT INTO etl_metrics (
        job_id, total_duration, target_row_count, max_updated_at, data_delay_minutes,
        source_row_count, extract_duration, load_duration, peak_memory_mb, created_at
    ) VALUES (
        :job_id, :total_duration, :target_row_count, :max_updated_at, :data_delay_minutes,
        :source_row_count, :extract_duration, :load_duration, :peak_memory_mb, NOW()
    )
    """
    
    metrics['job_id'] = job_id
    
    with engine.begin() as conn:
        conn.execute(text(create_sql))
        conn.execute(text(insert_sql), metrics)