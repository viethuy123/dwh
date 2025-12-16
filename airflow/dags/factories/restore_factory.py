# factories/restore_factory.py
"""
Factory để tạo restore tasks (Dropbox → MySQL local)
"""
from airflow.sdk import Variable, TaskGroup
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime, timedelta
from utils.dropbox_actions import generate_dropbox_access_token, file_exists, download_backup_file
import os


def _generate_and_push_token(**kwargs):
    """Tạo Dropbox access token và push vào XCom"""
    # Import config tại runtime
    from config import get_dropbox_config
    dropbox_config = get_dropbox_config()
    
    access_token = generate_dropbox_access_token(
        app_key=dropbox_config['app_key'],
        app_secret=dropbox_config['app_secret'],
        refresh_token=dropbox_config['refresh_token']
    )
    
    kwargs['ti'].xcom_push(key='dropbox_access_token', value=access_token)
    return access_token


def _create_download_backup_callable(backup_filename_template: str):
    """Factory function để tạo download backup callable"""
    def download_backup(**kwargs):
        # Import config tại runtime
        from config import get_dropbox_config
        dropbox_config = get_dropbox_config()
        
        # Get token từ XCom
        access_token = kwargs['ti'].xcom_pull(
            task_ids='restore.push_dropbox_access_token',
            key='dropbox_access_token'
        )
        
        # Tạo filename với yesterday date
        yesterday_str = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
        backup_filename = backup_filename_template.format(date=yesterday_str)
        filename_without_ext = backup_filename.rsplit('.', 1)[0]
        
        # Check file exists
        file_exist, file_path = file_exists(
            filename=filename_without_ext,
            access_token=access_token,
        )
        
        if not file_exist:
            raise FileNotFoundError(f"Backup file '{filename_without_ext}' not found in Dropbox")
        
        # Download
        download_path = os.path.join(dropbox_config['backup_local_dir'], backup_filename)
        download_backup_file(
            file_path=file_path,
            download_path=download_path,
            access_token=access_token
        )
        
        print(f"Successfully downloaded backup to {download_path}")
        return download_path
    
    return download_backup


def create_restore_task_group(dag, source_key: str, restore_config: dict) -> TaskGroup:
    """
    Tạo TaskGroup cho restore tasks
    
    Args:
        dag: DAG object
        source_key: Key của source trong PIPELINE_CONFIGS (vd: 'jira')
        restore_config: Restore config từ PIPELINE_CONFIGS
    
    Returns:
        TaskGroup chứa các restore tasks
    """
    with TaskGroup(group_id='restore', dag=dag) as restore_group:
        
        # Task 1: Push Dropbox access token
        push_token = PythonOperator(
            task_id='push_dropbox_access_token',
            python_callable=_generate_and_push_token,
            dag=dag,
        )
        
        # Task 2: Download backup file
        download_backup = PythonOperator(
            task_id='download_backup_file',
            python_callable=_create_download_backup_callable(
                restore_config['backup_filename_template']
            ),
            dag=dag,
        )
        
        # Task 3: Unzip file
        unzip_file = BashOperator(
            task_id='unzip_backup_file',
            bash_command=f"""
                echo "Unzipping {source_key} backups and deleting previous backups..."
                python3 {restore_config['unzip_script']}
            """,
            dag=dag,
        )
        
        # Task 4: Restore database
        restore_db = BashOperator(
            task_id=f"restore_{restore_config['db_name']}",
            bash_command=f"""
                echo "Restoring {restore_config['db_name']} database..."
                {restore_config['restore_script']}
            """,
            env={"MYSQL_PWD": Variable.get(restore_config['db_password_var'])},
            dag=dag,
        )
        
        # Define dependencies
        push_token >> download_backup >> unzip_file >> restore_db
    
    return restore_group


# """
# Factory để tạo restore tasks (Dropbox → MySQL local) - OPTIMIZED FOR SPEED
# """
# from airflow.sdk import Variable, TaskGroup
# from airflow.providers.standard.operators.python import PythonOperator
# from airflow.providers.standard.operators.bash import BashOperator
# from datetime import datetime, timedelta
# from utils.dropbox_actions import generate_dropbox_access_token, file_exists
# import os
# import subprocess
# import dropbox


# def _generate_and_push_token(**kwargs):
#     """Tạo Dropbox access token và push vào XCom"""
#     from config import get_dropbox_config
#     dropbox_config = get_dropbox_config()
    
#     access_token = generate_dropbox_access_token(
#         app_key=dropbox_config['app_key'],
#         app_secret=dropbox_config['app_secret'],
#         refresh_token=dropbox_config['refresh_token']
#     )
    
#     kwargs['ti'].xcom_push(key='dropbox_access_token', value=access_token)
#     return access_token


# def _get_backup_info(backup_filename_template: str, ti):
#     """Lấy thông tin backup file từ Dropbox"""
#     from config import get_dropbox_config
    
#     access_token = ti.xcom_pull(
#         task_ids='restore.push_dropbox_access_token',
#         key='dropbox_access_token'
#     )
    
#     yesterday_str = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
#     backup_filename = backup_filename_template.format(date=yesterday_str)
#     filename_without_ext = backup_filename.rsplit('.', 1)[0]
    
#     file_exist, file_path = file_exists(
#         filename=filename_without_ext,
#         access_token=access_token,
#     )
    
#     if not file_exist:
#         raise FileNotFoundError(
#             f"Backup file '{filename_without_ext}' not found in Dropbox"
#         )
    
#     return file_path, backup_filename, access_token


# def _decompress_with_pigz(input_stream, output_file):
#     """Decompress sử dụng pigz"""
#     pigz_process = subprocess.Popen(
#         ['pigz', '-d', '-c'],
#         stdin=subprocess.PIPE,
#         stdout=open(output_file, 'wb'),
#         stderr=subprocess.PIPE
#     )
    
#     for chunk in input_stream.iter_content(chunk_size=8192):
#         if chunk:
#             pigz_process.stdin.write(chunk)
    
#     pigz_process.stdin.close()
#     pigz_process.wait()
    
#     if pigz_process.returncode != 0:
#         error = pigz_process.stderr.read().decode()
#         raise Exception(f"Pigz decompression failed: {error}")


# def _handle_zip_file(temp_zip, output_dir):
#     """Xử lý file .zip"""
#     subprocess.run(['unzip', '-o', temp_zip, '-d', output_dir], check=True)
#     os.remove(temp_zip)


# def _stream_download_and_unzip(backup_filename_template: str, **kwargs):
#     """Stream download từ Dropbox và unzip trực tiếp bằng pigz"""
#     from config import get_dropbox_config
    
#     dropbox_config = get_dropbox_config()
#     ti = kwargs['ti']
    
#     # Get backup info
#     file_path, backup_filename, access_token = _get_backup_info(
#         backup_filename_template, ti
#     )
    
#     # Prepare output
#     output_dir = dropbox_config['backup_local_dir']
#     os.makedirs(output_dir, exist_ok=True)
    
#     if backup_filename.endswith('.gz'):
#         output_file = os.path.join(output_dir, backup_filename[:-3])
#     else:
#         output_file = os.path.join(output_dir, backup_filename.rsplit('.', 1)[0])
    
#     print(f"[INFO] Streaming: {file_path} -> {output_file}")
    
#     # Download and decompress
#     dbx = dropbox.Dropbox(access_token)
    
#     try:
#         metadata, response = dbx.files_download(file_path)
        
#         if backup_filename.endswith('.gz'):
#             _decompress_with_pigz(response, output_file)
#         elif backup_filename.endswith('.zip'):
#             temp_zip = os.path.join(output_dir, backup_filename)
#             with open(temp_zip, 'wb') as f:
#                 for chunk in response.iter_content(chunk_size=8192):
#                     if chunk:
#                         f.write(chunk)
#             _handle_zip_file(temp_zip, output_dir)
#         else:
#             with open(output_file, 'wb') as f:
#                 for chunk in response.iter_content(chunk_size=8192):
#                     if chunk:
#                         f.write(chunk)
        
#         print(f"[SUCCESS] Downloaded and unzipped: {output_file}")
#         ti.xcom_push(key='unzipped_file_path', value=output_file)
#         return output_file
        
#     except Exception as e:
#         print(f"[ERROR] Stream download failed: {e}")
#         raise


# def _build_restore_command(restore_config: dict) -> str:
#     """Tạo MySQL restore command với optimization"""
#     return f"""
#         echo "[INFO] Starting optimized restore for {restore_config['db_name']}..."
        
#         mysql -h {restore_config.get('db_host', 'dwh_mysql')} \\
#               -P{restore_config.get('db_port', 3306)} \\
#               -u{restore_config.get('db_user', 'root')} \\
#               {restore_config['db_name']} <<EOF
# SET FOREIGN_KEY_CHECKS=0;
# SET UNIQUE_CHECKS=0;
# SET AUTOCOMMIT=0;
# SET sql_log_bin=0;
# SET SESSION sort_buffer_size=268435456;
# SET SESSION read_buffer_size=8388608;
# SET SESSION read_rnd_buffer_size=16777216;

# SOURCE {{{{ ti.xcom_pull(task_ids='restore.stream_download_and_unzip', key='unzipped_file_path') }}}};

# SET FOREIGN_KEY_CHECKS=1;
# SET UNIQUE_CHECKS=1;
# COMMIT;
# EOF
        
#         echo "[SUCCESS] Restore completed!"
#     """


# def _build_verify_command(restore_config: dict) -> str:
#     """Tạo verify command"""
#     return f"""
#         echo "[INFO] Verifying restore..."
        
#         ROW_COUNT=$(mysql -N -h {restore_config.get('db_host', 'dwh_mysql')} \\
#               -P{restore_config.get('db_port', 3306)} \\
#               -u{restore_config.get('db_user', 'root')} \\
#               -e "SELECT SUM(TABLE_ROWS) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='{restore_config['db_name']}';")
        
#         if [[ -z "$ROW_COUNT" || "$ROW_COUNT" == "NULL" || "$ROW_COUNT" -eq 0 ]]; then
#             echo "[ERROR] Restore verification failed. Row count is zero or NULL."
#             exit 1
#         else
#             echo "[SUCCESS] Restore verified. Total rows: $ROW_COUNT"
#         fi
#     """


# def _create_download_task(dag, restore_config: dict) -> PythonOperator:
#     """Tạo download task"""
#     return PythonOperator(
#         task_id='stream_download_and_unzip',
#         python_callable=_stream_download_and_unzip,
#         op_kwargs={
#             'backup_filename_template': restore_config['backup_filename_template']
#         },
#         dag=dag,
#     )


# def _create_restore_task(dag, restore_config: dict) -> BashOperator:
#     """Tạo restore task"""
#     return BashOperator(
#         task_id=f"restore_{restore_config['db_name']}",
#         bash_command=_build_restore_command(restore_config),
#         env={"MYSQL_PWD": Variable.get(restore_config['db_password_var'])},
#         dag=dag,
#     )


# def _create_verify_task(dag, restore_config: dict) -> BashOperator:
#     """Tạo verify task"""
#     return BashOperator(
#         task_id=f"verify_{restore_config['db_name']}",
#         bash_command=_build_verify_command(restore_config),
#         env={"MYSQL_PWD": Variable.get(restore_config['db_password_var'])},
#         dag=dag,
#     )


# def create_restore_task_group(
#     dag, 
#     source_key: str, 
#     restore_config: dict
# ) -> TaskGroup:
#     """
#     Tạo TaskGroup cho restore tasks - OPTIMIZED VERSION
    
#     Args:
#         dag: DAG object
#         source_key: Key của source (vd: 'jira')
#         restore_config: Config với keys:
#             - backup_filename_template
#             - db_name
#             - db_password_var
#             - db_host (optional)
#             - db_port (optional)
#             - db_user (optional)
#     """
#     with TaskGroup(group_id='restore', dag=dag) as restore_group:
        
#         push_token = PythonOperator(
#             task_id='push_dropbox_access_token',
#             python_callable=_generate_and_push_token,
#             dag=dag,
#         )
        
#         download = _create_download_task(dag, restore_config)
#         restore = _create_restore_task(dag, restore_config)
#         verify = _create_verify_task(dag, restore_config)
        
#         push_token >> download >> restore >> verify
    
#     return restore_group