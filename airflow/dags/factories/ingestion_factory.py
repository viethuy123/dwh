"""
Factory để tạo ingestion tasks (Source → Staging PostgreSQL)
REFACTORED VERSION - Unified logic với chunking support cho tất cả sources
"""
from airflow.sdk import TaskGroup
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
from sqlalchemy import create_engine, text
from utils.extract_data import extract_sql_data, extract_mongo_data
from utils.data_transformers import get_transformer
from utils.common_tasks import (
    create_data_quality_check_callable,
    create_data_notification_callable,
    create_save_job_logs_callable
)
from config import get_pool_name, get_target_table_name
import pandas as pd
import gc

import csv
from io import StringIO

def psql_insert_copy(table, conn, keys, data_iter):
    """
    Hàm helper để dùng lệnh COPY của Postgres thay vì INSERT (Tăng tốc độ load)
    """
    dbapi_conn = conn.connection
    with dbapi_conn.cursor() as cur:
        s_buf = StringIO()
        writer = csv.writer(s_buf)
        writer.writerows(data_iter)
        s_buf.seek(0)

        columns = ', '.join('"{}"'.format(k) for k in keys)
        if table.schema:
            table_name = '{}.{}'.format(table.schema, table.name)
        else:
            table_name = table.name

        sql = 'COPY {} ({}) FROM STDIN WITH CSV'.format(table_name, columns)
        cur.copy_expert(sql=sql, file=s_buf)


def _create_extract_load_callable(
    source_uri_fn, 
    target_schema: str, 
    source_type: str,
    source_db: str = None
):
    """
    Universal factory để tạo extract & load callable cho tất cả source types
    TẤT CẢ đều hỗ trợ chunking và full load
    
    Args:
        source_uri_fn: Function trả về source URI
        target_schema: Schema đích trong staging
        source_type: 'mysql', 'mongodb', 'postgresql'
        source_db: Database name (bắt buộc cho MongoDB)
    """
    def extract_load_data(src_table: str, tgt_table: str, chunk_size: int = None) -> None:
        # 1. Lấy URIs
        source_uri = source_uri_fn()
        from config import DB_URIS
        staging_uri = DB_URIS['staging']()
        
        # 2. Setup PostgreSQL engine
        pg_engine = create_engine(staging_uri)
        transformer = get_transformer(source_type)
        
        # 3. Create schema if not exists
        with pg_engine.begin() as conn:
            conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {target_schema}"))
        
        print(f"Start loading {src_table} from {source_type}. Mode: {'Chunking ' + str(chunk_size) if chunk_size else 'Full Load'}")
        
        # 4. UNIFIED Extract & Load logic
        if chunk_size and source_type != 'mongodb':
            # === CHUNKING MODE - Chỉ cho SQL sources (MySQL, PostgreSQL) ===
            _load_with_chunking(
                source_uri=source_uri,
                source_type=source_type,
                src_table=src_table,
                pg_engine=pg_engine,
                tgt_table=tgt_table,
                target_schema=target_schema,
                transformer=transformer,
                chunk_size=chunk_size
            )
        else:
            # === FULL LOAD MODE - Áp dụng cho MongoDB hoặc SQL không chunk ===
            _load_full_table(
                source_uri=source_uri,
                source_type=source_type,
                source_db=source_db,
                src_table=src_table,
                pg_engine=pg_engine,
                tgt_table=tgt_table,
                target_schema=target_schema,
                transformer=transformer
            )
        
        return None
    
    return extract_load_data


def _load_with_chunking(
    source_uri: str,
    source_type: str,
    src_table: str,
    pg_engine,
    tgt_table: str,
    target_schema: str,
    transformer,
    chunk_size: int
):
    """
    Chunking logic chỉ cho SQL sources (MySQL, PostgreSQL)
    MongoDB sẽ dùng full load
    """
    src_engine = create_engine(source_uri, pool_pre_ping=True)
    is_first_chunk = True
    
    with src_engine.connect().execution_options(stream_results=True) as src_conn:
        df_iterator = pd.read_sql(
            f"SELECT * FROM {src_table} ORDER BY ID ASC",
            src_conn,
            chunksize=chunk_size
        )
        
        for i, df_chunk in enumerate(df_iterator):
            df_chunk = df_chunk.map(transformer)
            df_chunk['etl_datetime'] = datetime.now()
            
            load_mode = 'replace' if is_first_chunk else 'append'
            is_first_chunk = False
            
            with pg_engine.begin() as pg_conn:
                df_chunk.to_sql(
                    tgt_table,
                    con=pg_conn,
                    if_exists=load_mode,
                    index=False,
                    schema=target_schema,
                    method=psql_insert_copy
                )
            
            print(f"Loaded SQL chunk {i+1} ({len(df_chunk)} rows). Mode: {load_mode}")
            
            del df_chunk
            gc.collect()


def _load_full_table(
    source_uri: str,
    source_type: str,
    source_db: str,
    src_table: str,
    pg_engine,
    tgt_table: str,
    target_schema: str,
    transformer
):
    """
    UNIFIED full load logic cho TẤT CẢ source types
    """
    # Extract data dựa vào source type
    if source_type == 'mongodb':
        df = extract_mongo_data(source_uri, source_db, src_table)
    else:
        # SQL sources (MySQL, PostgreSQL)
        df = extract_sql_data(source_uri, f"SELECT * FROM {src_table}")
    
    if df.empty:
        print(f"Source table {src_table} is empty")
        return
    
    # Transform và add timestamp
    df = df.map(transformer)
    df['etl_datetime'] = datetime.now()
    
    # Load vào PostgreSQL
    with pg_engine.begin() as conn:
        df.to_sql(
            tgt_table,
            con=conn,
            if_exists='replace',
            index=False,
            chunksize=5000,
            schema=target_schema,
            method='multi'
        )
    
    print(f"Loaded full table {src_table}: {len(df)} rows")


def create_ingestion_task_group(dag, source_key: str, ingestion_config: dict) -> tuple:
    """
    Tạo TaskGroup cho ingestion tasks
    
    Args:
        dag: DAG object
        source_key: Key của source trong PIPELINE_CONFIGS (vd: 'jira', 'create')
        ingestion_config: Ingestion config từ PIPELINE_CONFIGS
    
    Returns:
        tuple: (outer_group, sync_fdw_task)
    """
    source_type = ingestion_config['source_type']
    source_uri_fn = ingestion_config['source_uri_fn']
    source_db = ingestion_config.get('source_db')
    target_schema = ingestion_config['target_schema']
    tables = ingestion_config['tables']
    
    # Tạo UNIFIED extract_load callable
    extract_load_fn = _create_extract_load_callable(
        source_uri_fn=source_uri_fn,
        target_schema=target_schema,
        source_type=source_type,
        source_db=source_db
    )
    
    # Import DB_URIS
    from config import DB_URIS
    
    # Tạo callable functions
    data_quality_fn = create_data_quality_check_callable(target_schema, DB_URIS['staging'])
    data_notification_fn = create_data_notification_callable(
        target_schema,
        DB_URIS['staging'],
        f'{source_key}_to_staging'
    )
    save_logs_fn = create_save_job_logs_callable(source_key, 'staging')
    
    with TaskGroup(group_id=f'{source_key}_to_staging', dag=dag) as outer_group:
        
        # Tạo inner task group cho từng table
        for table_config in tables:
            src_table = table_config['name']
            tgt_table = get_target_table_name(src_table, source_key)
            table_type = table_config['type']
            chunk_size = table_config.get('chunksize')  # None = full load
            pool = get_pool_name(table_type)
            
            with TaskGroup(group_id=f'{src_table}_{tgt_table}', dag=dag) as inner_group:
                
                # Extract & Load task - UNIFIED cho tất cả sources
                extract_load = PythonOperator(
                    task_id=f'extract_load_{target_schema}-{tgt_table}',
                    python_callable=extract_load_fn,
                    op_kwargs={
                        'src_table': src_table, 
                        'tgt_table': tgt_table,
                        'chunk_size': chunk_size
                    },
                    pool=pool,
                    dag=dag,
                )
                
                # Success logs
                success_logs = PythonOperator(
                    task_id=f'success_save_logs_{target_schema}-{tgt_table}',
                    python_callable=save_logs_fn,
                    op_kwargs={
                        'src_table': src_table,
                        'tgt_table': tgt_table,
                        'status': 'SUCCESS'
                    },
                    dag=dag,
                )
                
                # Data quality check
                quality_check = PythonOperator(
                    task_id=f'{target_schema}-{tgt_table}_quality_check',
                    python_callable=data_quality_fn,
                    op_kwargs={'tgt_table': tgt_table},
                    dag=dag,
                )
                
                # Data notification
                notification = PythonOperator(
                    task_id=f'{target_schema}-{tgt_table}_notification',
                    python_callable=data_notification_fn,
                    op_kwargs={
                        'src_table': src_table,
                        'tgt_table': tgt_table
                    },
                    dag=dag,
                )
                
                # Failure logs
                failure_logs = PythonOperator(
                    task_id=f'failure_save_logs_{target_schema}-{tgt_table}',
                    python_callable=save_logs_fn,
                    op_kwargs={
                        'src_table': src_table,
                        'tgt_table': tgt_table,
                        'status': 'FAILURE'
                    },
                    trigger_rule='all_failed',
                    dag=dag,
                )
                
                # Dependencies
                extract_load >> success_logs >> quality_check >> notification
                extract_load >> failure_logs
    
    return outer_group