# utils/data_transformers.py
"""
Chứa các functions để transform data trước khi load vào PostgreSQL
"""
from bson import ObjectId
import json
import re

def convert_null_bytes(val):
    """Remove null bytes từ strings (MySQL → PostgreSQL)"""
    if isinstance(val, str):
        return val.replace('\x00', '')
    return val


def convert_object_ids_recursive(val):
    """Convert MongoDB ObjectId thành string (MongoDB → PostgreSQL)"""
    if isinstance(val, ObjectId):
        return str(val)
    if isinstance(val, dict):
        return {k: convert_object_ids_recursive(v) for k, v in val.items()}
    if isinstance(val, list):
        return [convert_object_ids_recursive(i) for i in val]
    return val


def serialize_complex_types(val):
    """Serialize dict/list thành JSON string"""
    if isinstance(val, dict) or isinstance(val, list):
        return json.dumps(val)
    return val


# ==================== COMPOSITE TRANSFORMERS ====================
def transform_mysql_data(val):
    """Transform data từ MySQL"""
    val = convert_null_bytes(val)
    return val


def transform_mongodb_data(val):
    """Transform data từ MongoDB"""
    val = convert_object_ids_recursive(val)
    val = serialize_complex_types(val)
    val = convert_null_bytes(val)
    return val


def get_transformer(source_type: str):
    """
    Trả về transformer function phù hợp với source type
    
    Args:
        source_type: 'mysql' hoặc 'mongodb'
    
    Returns:
        Function để transform data
    """
    transformers = {
        'mysql': transform_mysql_data,
        'postgresql': transform_mysql_data,
        'mongodb': transform_mongodb_data,
    }
    
    return transformers.get(source_type, lambda x: x)


def normalize_column_name(col_name: str) -> str:
    """Chuyển camelCase sang snake_case"""
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', col_name)
    s2 = re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1)
    return s2.lower()


def transform_dataframe(df, transformer):

    from config import get_local_now
    
    for col in df.columns:
        if df[col].dtype == 'object':
            df[col] = df[col].apply(transformer) 
    
    df['etl_datetime'] = get_local_now()
    return df




def add_columns_to_table(pg_engine, tgt_table: str, target_schema: str, new_columns: set):
    from sqlalchemy import text
    
    with pg_engine.begin() as conn:
        for col in new_columns:
            sql = f'ALTER TABLE {target_schema}.{tgt_table} ADD COLUMN IF NOT EXISTS "{col}" TEXT'
            conn.execute(text(sql))
            print(f"Added column: {col}")

