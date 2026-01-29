"""
Factory để tạo ingestion tasks - LAZY IMPORT VERSION
Giảm RAM khi DAG processor parse bằng cách defer imports vào runtime
"""
from airflow.sdk import TaskGroup
from airflow.providers.standard.operators.python import PythonOperator

# ✅ CHỈ import những thứ CẦN THIẾT cho DAG definition
# KHÔNG import: sqlalchemy, pandas, gc, datetime ở đây
# → Chúng sẽ được import BÊN TRONG callable functions

import csv
from io import StringIO


def psql_insert_copy(table, conn, keys, data_iter):
    """
    Hàm helper để dùng lệnh COPY của Postgres
    ✅ Hàm này nhẹ, không cần lazy import
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
    Universal factory với LAZY IMPORTS
    
    ✅ Closure này CHỈ giữ 4 tham số nhẹ:
    - source_uri_fn (function reference)
    - target_schema (string)
    - source_type (string)  
    - source_db (string hoặc None)
    
    ❌ KHÔNG giữ: modules, classes, heavy objects
    """
    def extract_load_data(src_table: str, tgt_table: str, chunk_size: int = None) -> None:
        # ✅ LAZY IMPORTS - Chỉ load khi task CHẠY, không phải khi parse DAG
        from datetime import datetime
        from sqlalchemy import create_engine, text, pool
        import gc
        
        pg_engine = None
        
        try:
            # Lấy URIs
            source_uri = source_uri_fn()
            from config import DB_URIS
            staging_uri = DB_URIS['staging']()
            
            # Engine với NullPool
            pg_engine = create_engine(
                staging_uri,
                poolclass=pool.NullPool,
                echo=False
            )
            
            # ✅ Lazy import transformer
            from utils.data_transformers import get_transformer
            transformer = get_transformer(source_type)
            
            # Create schema
            with pg_engine.begin() as conn:
                conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {target_schema}"))
            
            print(f"Start loading {src_table} from {source_type}. Mode: {'Chunking ' + str(chunk_size) if chunk_size else 'Full Load'}")
            
            # Load logic - helper functions ở NGOÀI để tránh nested closures
            if chunk_size and source_type != 'mongodb':
                _load_with_chunking_internal(
                    source_uri, source_type, src_table,
                    pg_engine, tgt_table, target_schema,
                    transformer, chunk_size
                )
            else:
                _load_full_table_internal(
                    source_uri, source_type, source_db,
                    src_table, pg_engine, tgt_table,
                    target_schema, transformer
                )
        
        finally:
            if pg_engine:
                pg_engine.dispose()
            
            # Cleanup
            del transformer, source_uri, staging_uri
            for _ in range(3):
                gc.collect()
        
        return None
    
    return extract_load_data


# ✅ HELPER FUNCTIONS - Module level, KHÔNG phải nested closures
# Điều này giảm memory footprint vì không tạo closure mới cho mỗi table

def _load_with_chunking_internal(
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
    ✅ Helper function - KHÔNG phải closure
    ✅ Lazy imports bên trong
    """
    # ✅ Lazy imports
    from sqlalchemy import create_engine, pool
    from datetime import datetime
    import pandas as pd
    import gc
    
    src_engine = create_engine(
        source_uri,
        poolclass=pool.NullPool,
        echo=False
    )
    
    is_first_chunk = True
    processed_rows = 0
    
    try:
        with src_engine.connect().execution_options(
            stream_results=True,
            max_row_buffer=chunk_size
        ) as src_conn:
            
            df_iterator = pd.read_sql(
                f"SELECT * FROM {src_table} ORDER BY ID ASC",
                src_conn,
                chunksize=chunk_size
            )
            
            for i, df_chunk in enumerate(df_iterator):
                # ✅ Transform in-place thay vì .map()
                for col in df_chunk.columns:
                    if df_chunk[col].dtype == 'object':
                        df_chunk[col] = df_chunk[col].apply(transformer)
                
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
                
                processed_rows += len(df_chunk)
                print(f"✓ Loaded SQL chunk {i+1} ({len(df_chunk)} rows). Total: {processed_rows}")
                
                del df_chunk
                gc.collect()
        
        print(f"✓ Completed chunking: {processed_rows} rows")
    
    finally:
        src_engine.dispose()
        del src_engine
        gc.collect()


def _load_full_table_internal(
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
    ✅ Helper function - KHÔNG phải closure
    ✅ Lazy imports + batch processing
    """
    # ✅ Lazy imports
    from utils.extract_data import extract_sql_data, extract_mongo_data
    from datetime import datetime
    import pandas as pd
    import gc
    from sqlalchemy import TEXT
    
    df = None
    
    try:
        # Extract
        if source_type == 'mongodb':
            df = extract_mongo_data(source_uri, source_db, src_table)
        else:
            df = extract_sql_data(source_uri, f"SELECT * FROM {src_table}")
        
        if df is None or df.empty:
            print(f"Source table {src_table} is empty")
            return
        
        total_rows = len(df)
        print(f"Extracted {total_rows} rows from {src_table}")
        for col in df.columns:
            if df[col].isnull().all():
                df[col] = df[col].astype(object)
            
            if df[col].dtype == 'object':
                df[col] = df[col].apply(transformer)
                
            if any(key in col.lower() for key in ['date', 'time', 'at']) and df[col].dtype != 'object':
                 df[col] = df[col].astype(object)

        df['etl_datetime'] = datetime.now()
        
        # ✅ Batch processing
        BATCH_SIZE = 5000
        
        for batch_num, start_idx in enumerate(range(0, total_rows, BATCH_SIZE)):
            end_idx = min(start_idx + BATCH_SIZE, total_rows)
            df_batch = df.iloc[start_idx:end_idx].copy()
            
            # Transform in-place
            # for col in df_batch.columns:
            #     if df_batch[col].dtype == 'object':
            #         df_batch[col] = df_batch[col].apply(transformer)
            
            # df_batch['etl_datetime'] = datetime.now()
            
            load_mode = 'replace' if batch_num == 0 else 'append'
            
            with pg_engine.begin() as conn:
                df_batch.to_sql(
                    tgt_table,
                    con=conn,
                    if_exists=load_mode,
                    index=False,
                    schema=target_schema,
                    method=psql_insert_copy
                )
            
            print(f"✓ Loaded batch {batch_num + 1}: {len(df_batch)} rows ({end_idx}/{total_rows})")
            
            del df_batch
            gc.collect()
        
        print(f"✓ Completed full load: {total_rows} rows")
    
    finally:
        if df is not None:
            del df
        for _ in range(3):
            gc.collect()


def create_ingestion_task_group(dag, source_key: str, ingestion_config: dict) -> tuple:
    """
    Tạo TaskGroup với LAZY LOADING
    
    ✅ Chỉ import config module ở đây
    ✅ Không import heavy modules (sqlalchemy, pandas, etc)
    """
    source_type = ingestion_config['source_type']
    source_uri_fn = ingestion_config['source_uri_fn']
    source_db = ingestion_config.get('source_db')
    target_schema = ingestion_config['target_schema']
    tables = ingestion_config['tables']
    
    # ✅ Tạo callable - closure nhẹ
    extract_load_fn = _create_extract_load_callable(
        source_uri_fn=source_uri_fn,
        target_schema=target_schema,
        source_type=source_type,
        source_db=source_db
    )
    
    # ✅ Import config CHỈ khi cần
    from config import DB_URIS, get_pool_name, get_target_table_name
    
    # ✅ Import common_tasks CHỈ khi cần
    from utils.common_tasks import (
        create_data_quality_check_callable,
        create_data_notification_callable,
        create_save_job_logs_callable
    )
    
    # Tạo callables - REUSE cho tất cả tables
    data_quality_fn = create_data_quality_check_callable(target_schema, DB_URIS['staging'])
    data_notification_fn = create_data_notification_callable(
        target_schema,
        DB_URIS['staging'],
        f'{source_key}_to_staging'
    )
    save_logs_fn = create_save_job_logs_callable(source_key, 'staging')
    
    with TaskGroup(group_id=f'{source_key}_to_staging', dag=dag) as outer_group:
        
        for table_config in tables:
            src_table = table_config['name']
            tgt_table = get_target_table_name(src_table, source_key)
            table_type = table_config['type']
            chunk_size = table_config.get('chunksize')
            pool = get_pool_name(table_type)
            
            with TaskGroup(group_id=f'{src_table}_{tgt_table}', dag=dag) as inner_group:
                
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
                
                quality_check = PythonOperator(
                    task_id=f'{target_schema}-{tgt_table}_quality_check',
                    python_callable=data_quality_fn,
                    op_kwargs={'tgt_table': tgt_table},
                    dag=dag,
                )
                
                notification = PythonOperator(
                    task_id=f'{target_schema}-{tgt_table}_notification',
                    python_callable=data_notification_fn,
                    op_kwargs={
                        'src_table': src_table,
                        'tgt_table': tgt_table
                    },
                    dag=dag,
                )
                
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
                
                extract_load >> success_logs >> quality_check >> notification
                extract_load >> failure_logs
    
    return outer_group