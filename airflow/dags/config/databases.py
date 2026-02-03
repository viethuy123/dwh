"""
Database connection URIs - MEMORY OPTIMIZED với SINGLETON
Lazy evaluation + singleton cache để giảm RAM khi DAG parsing
"""
from airflow.sdk import Variable
from functools import lru_cache
from typing import Dict, Callable


class _URIConfigSingleton:
    """
    ✅ SINGLETON pattern để đảm bảo chỉ có 1 instance duy nhất
    Tránh tạo nhiều cache instances khi module được import nhiều lần
    """
    _instance = None
    _initialized = False
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(_URIConfigSingleton, cls).__new__(cls)
        return cls._instance
    
    def __init__(self):
        # ✅ Chỉ initialize 1 lần duy nhất
        if not _URIConfigSingleton._initialized:
            self._variable_cache = {}
            _URIConfigSingleton._initialized = True
    
    def get_variable(self, key: str) -> str:
        """
        Get Airflow Variable với caching
        Cache trong singleton instance thay vì lru_cache
        """
        if key not in self._variable_cache:
            self._variable_cache[key] = Variable.get(key)
        return self._variable_cache[key]
    
    def clear_cache(self):
        """Clear toàn bộ cached variables"""
        self._variable_cache.clear()
        print("✓ Cleared URI variable cache")


# ✅ Global singleton instance
_uri_config = _URIConfigSingleton()


def _get_pg_uri(db_name: str) -> str:
    """Build PostgreSQL connection URI - với singleton cache"""
    return "postgresql+psycopg2://{}:{}@{}:{}/{}".format(
        _uri_config.get_variable("pg_user"),
        _uri_config.get_variable("pg_password"),
        _uri_config.get_variable("pg_host"),
        _uri_config.get_variable("pg_port"),
        db_name
    )


def _get_mysql_uri(db_name: str, prefix: str) -> str:
    """Build MySQL connection URI - với singleton cache"""
    return "mysql+pymysql://{}:{}@{}:{}/{}".format(
        _uri_config.get_variable(f"{prefix}_user"),
        _uri_config.get_variable(f"{prefix}_password"),
        _uri_config.get_variable(f"{prefix}_host"),
        _uri_config.get_variable(f"{prefix}_port"),
        db_name
    )


def _get_mongo_uri(db_name: str, prefix: str) -> str:
    """Build MongoDB connection URI - với singleton cache"""
    return "mongodb://{}:{}@{}:{}/{}".format(
        _uri_config.get_variable(f"{prefix}_user"),
        _uri_config.get_variable(f"{prefix}_password"),
        _uri_config.get_variable(f"{prefix}_host"),
        _uri_config.get_variable(f"{prefix}_port"),
        db_name
    )


# ✅ Lazy evaluation - functions KHÔNG được call khi import module
DB_URIS: Dict[str, Callable[[], str]] = {
    'dwh': lambda: _get_pg_uri("dwh"),
    'monitoring': lambda: _get_pg_uri("monitoring")
}


def get_mysql_uri_builder(db_name: str, prefix: str) -> Callable[[], str]:
    """
    Return a lazy function to build MySQL URI
    """
    return lambda: _get_mysql_uri(db_name, prefix)


def get_mongo_uri_builder(db_name: str, prefix: str) -> Callable[[], str]:
    """
    Return a lazy function to build MongoDB URI
    """
    return lambda: _get_mongo_uri(db_name, prefix)


def clear_uri_cache():
    """
    ✅ Public function để clear cache từ bên ngoài
    """
    _uri_config.clear_cache()


# ✅ OPTIONAL: Function để pre-warm cache (nếu cần)
def prewarm_cache():
    """
    Pre-load tất cả variables vào cache
    Gọi 1 lần khi DAG start để tránh cold start
    """
    common_vars = [
        "pg_user", "pg_password", "pg_host", "pg_port"
    ]
    for var in common_vars:
        try:
            _uri_config.get_variable(var)
        except:
            pass
    print("✓ Pre-warmed URI cache")