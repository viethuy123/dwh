"""
Configuration package for Airflow DAGs
"""
from config.common import DEFAULT_ARGS, get_pool_name, get_target_table_name, DEFAULT_CHECK_DAG
from config.databases import DB_URIS
from config.external import get_slack_config, get_dropbox_config
from config.sources import SOURCES
from config.dbt import DBT_PIPELINES, DBT_CONFIG

__all__ = [
    'DEFAULT_ARGS',
    'DEFAULT_CHECK_DAG',
    'DB_URIS',
    'get_slack_config',
    'get_dropbox_config',
    'DBT_CONFIG',
    'SOURCES',
    'DBT_PIPELINES',
    'get_pool_name',
    'get_target_table_name',
]