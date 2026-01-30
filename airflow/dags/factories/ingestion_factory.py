"""
Factory để tạo ingestion tasks - LAZY IMPORT VERSION
"""
from airflow.sdk import TaskGroup
from airflow.providers.standard.operators.python import PythonOperator
import csv
from io import StringIO
import re


# ===== HELPER FUNCTIONS =====



def psql_insert_copy(table, conn, keys, data_iter):
    """Hàm helper để dùng lệnh COPY của Postgres"""
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


def load_chunk_to_postgres(df_chunk, pg_engine, tgt_table: str, target_schema: str, load_mode: str, dtype: dict = None):
    with pg_engine.begin() as pg_conn:
        df_chunk.to_sql(
            tgt_table,
            con=pg_conn,
            if_exists=load_mode,
            index=False,
            schema=target_schema,
            dtype=dtype,
            method=psql_insert_copy
        )


def _load_with_chunking_smart(
    source_uri: str,
    source_type: str,
    src_table: str,
    pg_engine,
    tgt_table: str,
    target_schema: str,
    transformer,
    chunk_size: int,
    source_db: str = None
):

    from utils.extract_data import extract_mongo_data_chunked, extract_sql_data_chunked
    from utils.data_transformers import normalize_column_name, transform_dataframe, add_columns_to_table
    import gc
    
    # Get iterator
    if source_type == 'mongodb':
        chunk_iterator = extract_mongo_data_chunked(source_uri, source_db, src_table, chunk_size)
    else:
        chunk_iterator = extract_sql_data_chunked(
            source_uri, 
            f"SELECT * FROM {src_table} ORDER BY ID ASC", 
            chunk_size
        )
    
    is_first_chunk = True
    processed_rows = 0
    known_columns = set()
    
    for i, df_chunk in enumerate(chunk_iterator):
        
        # Normalize column names
        # column_mapping = {col: normalize_column_name(col) for col in df_chunk.columns}
        # df_chunk.rename(columns=column_mapping, inplace=True)
        if source_type == 'mongodb':
            for col in df_chunk.columns:
                if col != 'etl_datetime':  # Giữ nguyên etl_datetime
                    df_chunk[col] = df_chunk[col].astype(str)
                    df_chunk[col] = df_chunk[col].replace('nan', None)
                    df_chunk[col] = df_chunk[col].replace('None', None)
        
        df_chunk = transform_dataframe(df_chunk, transformer)
        
        if is_first_chunk:
            # Chunk đầu: tạo table
            if source_type == 'mongodb':
                from sqlalchemy.types import TEXT
                dtype_dict = {col: TEXT for col in df_chunk.columns if col != 'etl_datetime'}
                
                with pg_engine.begin() as pg_conn:
                    df_chunk.to_sql(
                        tgt_table,
                        con=pg_conn,
                        if_exists='replace',
                        index=False,
                        schema=target_schema,
                        dtype=dtype_dict,
                        method=psql_insert_copy
                    )
            else:
                load_chunk_to_postgres(df_chunk, pg_engine, tgt_table, target_schema, 'replace')
            
            known_columns = set(df_chunk.columns)
            is_first_chunk = False
        else:
            # Chunk sau: kiểm tra cột mới
            new_columns = set(df_chunk.columns) - known_columns
            
            if new_columns:
                print(f"Found new columns: {new_columns}")
                add_columns_to_table(pg_engine, tgt_table, target_schema, new_columns)
                known_columns.update(new_columns)
            
            # Fill missing columns
            for col in known_columns:
                if col not in df_chunk.columns:
                    df_chunk[col] = None
            
            # Đảm bảo thứ tự columns
            df_chunk = df_chunk[sorted(known_columns)]
            
            load_chunk_to_postgres(df_chunk, pg_engine, tgt_table, target_schema, 'append')
        
        processed_rows += len(df_chunk)
        print(f"✓ Chunk {i+1}: {len(df_chunk)} rows | Total: {processed_rows}")
        
        del df_chunk
        gc.collect()
    
    print(f"Completed chunking: {processed_rows} rows from {source_type}")


# ===== FULL LOAD =====

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

    from utils.extract_data import extract_sql_data, extract_mongo_data
    from utils.data_transformers import normalize_column_name, transform_dataframe
    import pandas as pd
    import gc
    
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
        
        # Normalize columns
        # column_mapping = {col: normalize_column_name(col) for col in df.columns}
        # df.rename(columns=column_mapping, inplace=True)
        
        # Handle data types
        for col in df.columns:
            if df[col].isnull().all():
                df[col] = df[col].astype(object)
            
            if any(key in col.lower() for key in ['date', 'time', 'at']) and df[col].dtype != 'object':
                df[col] = df[col].astype(object)
        
        df = transform_dataframe(df, transformer)
        
        # Batch load
        BATCH_SIZE = 5000
        
        for batch_num, start_idx in enumerate(range(0, total_rows, BATCH_SIZE)):
            end_idx = min(start_idx + BATCH_SIZE, total_rows)
            df_batch = df.iloc[start_idx:end_idx].copy()
            
            load_mode = 'replace' if batch_num == 0 else 'append'
            # load_chunk_to_postgres(df_batch, pg_engine, tgt_table, target_schema, load_mode)
            with pg_engine.begin() as conn:
                df_batch.to_sql(
                    tgt_table,
                    con=conn,
                    if_exists=load_mode,
                    index=False,
                    schema=target_schema,
                    method=psql_insert_copy
                )
            
            print(f"✓ Batch {batch_num + 1}: {len(df_batch)} rows | Progress: {end_idx}/{total_rows}")
            
            del df_batch
            gc.collect()
        
        print(f"Completed full load: {total_rows} rows")
    
    finally:
        if df is not None:
            del df
        for _ in range(3):
            gc.collect()


# ===== CALLABLE FACTORY =====

def _create_extract_load_callable(
    source_uri_fn, 
    target_schema: str, 
    source_type: str,
    source_db: str = None
):
    def extract_load_data(src_table: str, tgt_table: str, chunk_size: int = None) -> None:
        from datetime import datetime
        from sqlalchemy import create_engine, text, pool
        import gc
        
        pg_engine = None
        
        try:
            # Get URIs
            source_uri = source_uri_fn()
            from config import DB_URIS
            staging_uri = DB_URIS['staging']()
            
            # Create engine
            pg_engine = create_engine(
                staging_uri,
                poolclass=pool.NullPool,
                echo=False
            )
            
            from utils.data_transformers import get_transformer
            transformer = get_transformer(source_type)
            
            # Create schema
            with pg_engine.begin() as conn:
                conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {target_schema}"))
            
            print(f"Start loading {src_table} from {source_type}")
            print(f"Mode: {'Chunking ' + str(chunk_size) if chunk_size else 'Full Load'}")
            
            # Load logic
            if chunk_size:
                _load_with_chunking_smart(  
                    source_uri, source_type, src_table,
                    pg_engine, tgt_table, target_schema,
                    transformer, chunk_size, source_db
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
            
            del transformer, source_uri, staging_uri
            for _ in range(3):
                gc.collect()
        
        return None
    
    return extract_load_data

def create_ingestion_task_group(dag, source_key: str, ingestion_config: dict) -> tuple:
    source_type = ingestion_config['source_type']
    source_uri_fn = ingestion_config['source_uri_fn']
    source_db = ingestion_config.get('source_db')
    target_schema = ingestion_config['target_schema']
    tables = ingestion_config['tables']
    
    extract_load_fn = _create_extract_load_callable(
        source_uri_fn=source_uri_fn,
        target_schema=target_schema,
        source_type=source_type,
        source_db=source_db
    )
    
    from config import DB_URIS, get_pool_name, get_target_table_name
    from utils.common_tasks import (
        create_data_quality_check_callable,
        create_data_notification_callable,
        create_save_job_logs_callable
    )
    
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