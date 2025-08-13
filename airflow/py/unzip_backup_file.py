import zipfile
import os
import shutil
from datetime import datetime, timedelta


backup_root = "/opt/airflow/database_backup"
yesterday = datetime.now() - timedelta(days=1)
yesterday_str = yesterday.strftime("%Y%m%d")
zip_path = f"{backup_root}/jira8db_bk_{yesterday_str}_22.zip"
extract_dir = f"{backup_root}/jira8db_bk_{yesterday_str}_22"

os.makedirs(extract_dir, exist_ok=True)

with zipfile.ZipFile(zip_path, 'r') as zip_ref:
    zip_ref.extractall(extract_dir)

print(f"Unzipped {zip_path} to {extract_dir}")

os.remove(zip_path)

for name in os.listdir(backup_root):
    folder_path = os.path.join(backup_root, name)
    if os.path.isdir(folder_path) and name.startswith("jira8db_bk_"):
        try:
            date_str = name.split("_")[2]
            folder_date = datetime.strptime(date_str, "%Y%m%d")
            if folder_date.date() < yesterday.date():
                shutil.rmtree(folder_path)
                print(f"Removed old backup folder: {folder_path}")
        except (IndexError, ValueError):
            pass  