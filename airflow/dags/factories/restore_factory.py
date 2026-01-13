# factories/restore_factory.py
"""
Factory để tạo restore tasks (Dropbox → MySQL local)
"""
from airflow.sdk import Variable, TaskGroup
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
import os


def _download_logic(backup_template, target_date, **kwargs):
    from config import get_dropbox_config
    from utils.dropbox_actions import generate_dropbox_access_token, file_exists, download_backup_file
    
    dropbox_config = get_dropbox_config()
    # target_date nhận từ {{ ds_nodash }} của Airflow
    backup_filename = backup_template.format(date=target_date)
    download_path = os.path.join(dropbox_config['backup_local_dir'], backup_filename)

    # TỐI ƯU: Kiểm tra file tồn tại ở local
    if os.path.exists(download_path) and os.path.getsize(download_path) > 0:
        print(f"[SKIP] File {backup_filename} đã tồn tại tại local. Không tải lại.")
        return download_path

    print(f"[INFO] File không có ở local. Đang khởi tạo tải từ Dropbox cho ngày {target_date}...")
    
    access_token = generate_dropbox_access_token(
        app_key=dropbox_config['app_key'],
        app_secret=dropbox_config['app_secret'],
        refresh_token=dropbox_config['refresh_token']
    )
    
    filename_no_ext = backup_filename.rsplit('.', 1)[0]
    exists, remote_path = file_exists(filename=filename_no_ext, access_token=access_token)
    
    if not exists:
        raise FileNotFoundError(f"Không tìm thấy file {filename_no_ext} trên Dropbox")

    download_backup_file(remote_path, download_path, access_token)
    return download_path


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
        download_task = PythonOperator(
            task_id='download_backup_file',
            python_callable=_download_logic,
            op_kwargs={
                'backup_template': restore_config['backup_filename_template'],
                'target_date': "{{ (macros.ds_add(ds, -1)) | replace('-', '') }}"},
        )
        
        # Task 2: Unzip file
        unzip_task = BashOperator(
            task_id='unzip_backup_file',
            # Dấu cách cuối chuỗi cực kỳ quan trọng để tránh lỗi TemplateNotFound
            bash_command=f"python3 {restore_config['unzip_script']} --file {{{{ ti.xcom_pull(task_ids='restore.download_backup_file') }}}} ",
        )
        
        # Task 3: Restore database
        restore_db = BashOperator(
            task_id=f"restore_db_task",
            bash_command=f"bash {restore_config['restore_script']} ",
            env={
                "MYSQL_PWD": Variable.get(restore_config['db_password_var']),
                "DB_NAME": restore_config['db_name'],
                "YESTERDAY_STR": "{{ (macros.ds_add(ds, -1)) | replace('-', '') }}"
            },
        )
        
        # Define dependencies
        download_task >> unzip_task >> restore_db
    
    return restore_group


