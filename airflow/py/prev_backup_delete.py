from datetime import datetime, timedelta
import re
from minio import Minio
from minio.error import S3Error

def main():
    
    client = Minio(
            "minio:9000",
            access_key="admin",
            secret_key="admin123",
            secure=False
        )
    
    today = datetime.now().date()

    bucket_name = "database-backup"

    date_pattern = re.compile(r"(\d{8})")

    to_delete = []
    for obj in client.list_objects(bucket_name, recursive=True):
        match = date_pattern.search(obj.object_name) # type: ignore
        if match:
            try:
                obj_date = datetime.strptime(match.group(1), "%Y%m%d").date()
                if obj_date < today - timedelta(days=1):
                    to_delete.append(obj.object_name)
            except ValueError:
                pass

    for key in to_delete:
        client.remove_object(bucket_name, key)
        print(f"Deleted: {key}")

    print(f"Total deleted: {len(to_delete)}")
    
    
if __name__ == "__main__":
    try:
        main()
    except S3Error as exc:
        print("error occurred.", exc)
