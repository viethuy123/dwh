# Triển khai DWH bằng Docker Compose

## Mô tả

Dự án này sử dụng Docker Compose để triển khai các services cho DWH.

## Yêu cầu

- Docker
- Docker Compose

## Cách triển khai

```bash
cd airflow && docker compose up airflow-init && docker compose up -d --build # airflow

cd ../postgre && docker compose up -d # psql warehouse storage

cd ../minio && docker compose up -d # s3 

cd ../metabase && docker compose up -d # BI service

## Dừng cách dịch vụ

cd ${project_folder} && docker compose down --rmi all --volumes --remove-orphans 