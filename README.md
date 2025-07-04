# Triển khai DWH bằng Docker Compose

## Mô tả

Dự án này sử dụng Docker Compose để triển khai các services cho DWH.

## Yêu cầu

- Docker
- Docker Compose

## Cách triển khai

```bash
## Triển khai docker compose

cd airflow && docker compose up airflow-init && docker compose up -d --build # airflow

cd ../postgre && docker compose up -d # psql warehouse storage

cd ../minio && docker compose up -d # s3 

cd ../metabase && docker compose up -d # BI service

## Dừng docker compose

cd ${project_folder} && docker compose down --rmi all --volumes --remove-orphans 
```
## Lưu ý

Để chạy great_expectations cần exec vào container của airflow và kích hoạt great_expectations init

``` bash
docker exec -it airflow-airflow-worker-1 /bin/bash

cd great_expectations && python3 -c "from great_expectations.data_context import get_context; get_context()"
```