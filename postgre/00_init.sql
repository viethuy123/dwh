CREATE DATABASE stg;
CREATE DATABASE dwh;
CREATE DATABASE dtm;
CREATE DATABASE monitoring;

\c stg;
create schema src_jira;
create schema src_create;
create schema src_jisseki;

\c dwh;
create schema jira_fdw;
create schema create_fdw;
create schema jisseki_fdw;
create schema fdw_metadata;
CREATE EXTENSION IF NOT EXISTS postgres_fdw;
CREATE TABLE time_series (
    time_id SERIAL PRIMARY KEY,
    date DATE NOT NULL,
    year INT,
    month INT,
    month_name VARCHAR(10),
    quarter INT,
    is_month_end BOOLEAN
);
INSERT INTO time_series (date, year, month, month_name, quarter, is_month_end)
SELECT
    date_series AS date,
    EXTRACT(YEAR FROM date_series) AS year,
    EXTRACT(MONTH FROM date_series) AS month,
    TO_CHAR(date_series, 'Month') AS month_name,
    EXTRACT(QUARTER FROM date_series) AS quarter,
    CASE
        WHEN date_series = (DATE_TRUNC('month', date_series) + INTERVAL '1 month - 1 day')::DATE THEN TRUE
        ELSE FALSE
    END AS is_month_end
FROM generate_series('2000-01-01'::date, '2099-12-31'::date, interval '1 day') AS date_series;

\c dtm;
create schema dwh_fdw;
create schema fdw_metadata;
CREATE EXTENSION IF NOT EXISTS postgres_fdw;

\c monitoring;
CREATE TABLE etl_job_logs (
    id SERIAL PRIMARY KEY,
    job_name VARCHAR(255),
    source_db VARCHAR(255),
    target_db VARCHAR(255),
    source_table JSONB,
    target_table VARCHAR(255),
    dag_id VARCHAR(255),
    task_id VARCHAR(255),
    execution_time TIMESTAMP,
    status VARCHAR(50),
    created_time TIMESTAMP
);
