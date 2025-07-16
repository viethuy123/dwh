from datetime import datetime
from minio import Minio
from minio.error import S3Error

def main():

    today_str = datetime.now().strftime("%Y%m%d")

    client = Minio(
        "minio:9000",
        access_key="admin",
        secret_key="admin123",
        secure=False
    )

    source_files_list = [
        f"/opt/airflow/database_backup/warehouse/warehouse_databases_{today_str}.sql.gz",
        f"/opt/airflow/database_backup/metabase/metabase_database_{today_str}.sql.gz"
    ]

    bucket_name_list = ["database_backup/warehouse/","database_backup/metabase/"]

    for bucket_name in bucket_name_list:
        found = client.bucket_exists(bucket_name)
        if not found:
            client.make_bucket(bucket_name)
            print("Created bucket", bucket_name)
        else:
            print("Bucket", bucket_name, "already exists")

    for source_file, bucket_name in zip(source_files_list, bucket_name_list):
        object_name = source_file.split("/")[-1]
        client.fput_object(
            bucket_name, object_name, source_file,
        )
        print(
            source_file, "successfully uploaded as object",
            object_name, "to bucket", bucket_name,
        )

if __name__ == "__main__":
    try:
        main()
    except S3Error as exc:
        print("error occurred.", exc)