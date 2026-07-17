-- Migration script: tạo bảng monitoring cho ETL pipeline.
-- Chạy 1 lần trên monitoring database trước khi DAG sử dụng.
-- Idempotent — có thể chạy lại nhiều lần an toàn.

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
);

CREATE TABLE IF NOT EXISTS etl_metrics (
    id SERIAL PRIMARY KEY,
    job_id INTEGER REFERENCES etl_job_logs(id),
    total_duration FLOAT,
    target_row_count BIGINT,
    max_updated_at TIMESTAMP,
    data_delay_minutes FLOAT,
    source_row_count BIGINT,
    extract_duration FLOAT,
    load_duration FLOAT,
    peak_memory_mb FLOAT,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Index để query metrics theo job
CREATE INDEX IF NOT EXISTS idx_etl_metrics_job_id ON etl_metrics(job_id);

-- Index để query logs theo status và thời gian
CREATE INDEX IF NOT EXISTS idx_etl_job_logs_status ON etl_job_logs(status, created_at);
