"""dbt transformation pipeline configurations"""
from config.databases import _get_pg_uri
DBT_CONFIG = {
    'project_dir': '/opt/airflow/dbt',
    'profiles_dir': '/opt/airflow/dbt/.dbt/',
    'profile': 'dwh_project'
}
DBT_PIPELINES = {
    'staging_to_warehouse': {
        'dag_id': 'dag_transform_to_dwh',
        'schedule': '30 20 * * *',
        'timeout_minutes': 60,
        'source_db': 'dwh',
        'target_db': 'dwh',
        'dbt_target': 'dwh',
        'models_path': 'models/dwh',
        'tgt_schema': 'intermediates',
        'table_mapping_var': 'warehouse_mapping',
        'fdw_sync': {
            'enabled': False,
        }
    },
    
    'reports': {
        'dag_id': 'dag_reports',
        'schedule': '15 21 * * *',
        'timeout_minutes': 60,
        'source_db': 'dwh',
        'target_db': 'dwh',
        'dbt_target': 'dwh',
        'models_path': 'models/dwh',
        'tgt_schema': 'reports',
        'table_mapping_var': 'report_mapping',
        'fdw_sync': {
            'enabled': False,
        }
    },
    
    'dim_data': {
        'dag_id': 'dag_dim_data',
        'schedule': '50 20 * * *',
        'timeout_minutes': 60,
        'source_db': 'dwh',
        'target_db': 'dwh',
        'dbt_target': 'dwh',
        'models_path': 'models/dwh',
        'tgt_schema': 'dim',
        'table_mapping_var': 'dim_mapping',
        'fdw_sync': {
            'enabled': False,
        }
    },

    'fct_data': {
        'dag_id': 'dag_fct_data',
        'schedule': '0 21 * * *',
        'timeout_minutes': 60,
        'source_db': 'dwh',
        'target_db': 'dwh',
        'dbt_target': 'dwh',
        'models_path': 'models/dwh',
        'tgt_schema': 'fct',
        'table_mapping_var': 'fct_mapping',
        'fdw_sync': {
            'enabled': False,
        }
    },
    'snapshots': {
        'dag_id': 'dag_snapshots',  
        'schedule': '0 22 * * *',
        'timeout_minutes': 60,
        'source_db': 'dwh',
        'target_db': 'dwh',
        'dbt_target': 'dwh',
        'models_path': 'snapshots',
        'tgt_schema': 'snapshots',
        'method': 'snapshot',
        'table_mapping_var': 'snapshot_mapping',
        'fdw_sync': {
            'enabled': False,
        }
    },

    'dwh_to_jira_dtm': {
        'dag_id': 'dag_dwh_to_jira_dtm',
        'schedule': '50 20 * * *',
        'timeout_minutes': 60,
        'source_db': 'dtm',
        'target_db': 'dtm',
        'source_schema': 'dwh_fdw',
        'target_schema': 'jira',
        'dbt_target': 'dtm',
        'models_path': 'models/dtm/jira',
        'table_mapping_var': 'jira_dtm_mapping',
        'fdw_sync': {
            'enabled': False,
        }
    }
}