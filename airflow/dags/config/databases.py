"""Database connection URIs (lazy evaluation)"""
from airflow.sdk import Variable

def _get_pg_uri(db_name: str) -> str:
    """Build PostgreSQL connection URI"""
    return "postgresql+psycopg2://{}:{}@{}:{}/{}".format(
        Variable.get("pg_user"),
        Variable.get("pg_password"),
        Variable.get("pg_host"),
        Variable.get("pg_port"),
        db_name
    )

def _get_mysql_uri(db_name: str, prefix: str) -> str:
    """Build MySQL connection URI"""
    return "mysql+pymysql://{}:{}@{}:{}/{}".format(
        Variable.get(f"{prefix}_user"),
        Variable.get(f"{prefix}_password"),
        Variable.get(f"{prefix}_host"),
        Variable.get(f"{prefix}_port"),
        db_name
    )

def _get_mongo_uri(db_name: str, prefix: str) -> str:
    """Build MongoDB connection URI"""
    return "mongodb://{}:{}@{}:{}/{}".format(
        Variable.get(f"{prefix}_user"),
        Variable.get(f"{prefix}_password"),
        Variable.get(f"{prefix}_host"),
        Variable.get(f"{prefix}_port"),
        db_name
    )

# Lazy evaluation - functions not called until runtime
DB_URIS = {
    'staging': lambda: _get_pg_uri("dwh"),
    'monitoring': lambda: _get_pg_uri("monitoring")
}

# Expose builder functions for source configs
def get_mysql_uri_builder(db_name: str, prefix: str):
    """Return a lazy function to build MySQL URI"""
    return lambda: _get_mysql_uri(db_name, prefix)

def get_mongo_uri_builder(db_name: str, prefix: str):
    """Return a lazy function to build MongoDB URI"""
    return lambda: _get_mongo_uri(db_name, prefix)