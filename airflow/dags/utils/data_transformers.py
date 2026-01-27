# utils/data_transformers.py
"""
Chứa các functions để transform data trước khi load vào PostgreSQL
"""
from bson import ObjectId
import json


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
        'mongodb': transform_mongodb_data,
    }
    
    return transformers.get(source_type, lambda x: x)