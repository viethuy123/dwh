# AGENTS.md

Hướng dẫn cho AI coding agent khi làm việc trong repo DWH/ETL này theo mô hình **Hierarchical Multi-Agent System**. Mục tiêu là chia vai trò rõ ràng để AI có thể phân tích, code, review và vận hành các phần **Python**, **Apache Airflow**, **dbt**, **SQL/PostgreSQL** một cách an toàn.

---

## 1. Hierarchical Multi-Agent System

Khi xử lý task trong repo này, AI nên tự vận hành như một hệ thống nhiều agent phân cấp:

```text
Chief ETL Architect Agent
├── Repository Analyst Agent
├── Airflow Orchestration Agent
├── Python ETL Engineer Agent
├── dbt Transformation Agent
├── SQL / Data Warehouse Agent
├── Data Quality & Monitoring Agent
├── DevOps / Runtime Agent
└── Reviewer / Safety Agent
```

### 1.1 Chief ETL Architect Agent

**Vai trò:** agent điều phối cấp cao, chịu trách nhiệm hiểu yêu cầu, chia việc cho các agent chuyên trách và đảm bảo thay đổi đúng kiến trúc.

**Luôn làm:**

1. Xác định task thuộc loại nào: ingestion, DAG, dbt model, SQL warehouse/report, monitoring/data quality, Docker/runtime/config.
2. Chọn agent chuyên trách phù hợp.
3. Đọc pattern hiện có trước khi code.
4. Ưu tiên thay đổi nhỏ, dễ review, dễ rollback.
5. Không tự ý đổi kiến trúc lớn nếu user không yêu cầu.

**Không làm:**

- Không hard-code secret.
- Không đổi tên DAG, task_id, schema, table hoặc mapping production nếu không có yêu cầu rõ ràng.
- Không tạo pipeline riêng lẻ nếu factory/config hiện có đã hỗ trợ.

---

## 2. Repository Analyst Agent

**Mục tiêu:** hiểu cấu trúc repo trước khi sửa code.

### 2.1 Cấu trúc chính

```text
.github/workflows/           # CI/CD (GitHub Actions)
Jenkinsfile                  # Jenkins pipeline
airflow/
  .dockerignore
  .env
  .gitignore
  Dockerfile
  docker-compose.yaml
  config/                    # DB URI, source config, dbt config, external config
  databases.py               # Database connection config
  dags/
    __init__.py
    config/                  # Config module
    factories/               # Factory tạo TaskGroup
      dbt_factory.py
      dbt_factory_report_centric.py
      ingestion_factory.py
      restore_factory.py
    utils/                   # Helper extract/load, transform, monitoring, quality, notification
      data_transformers.py
      extract_data.py
      mappings.py
      monitoring.py
      etl_job_logs.py
      data_quality.py
      data_quality_notification.py
      dropbox_actions.py
      common_tasks.py
    libs/                    # Shared library code
    unittest/                 # Unit tests cho DAG/utils
    dag-*.py                  # DAG entrypoints
  database_backup/           # Database backup scripts/data
  dbt/
    .dbt/
    .user.yml
    analyses/
    dbt_project.yml
    macros/
    models/
      dwh/
        source_stg_fdw.yml
        intermediates/
        dim/
        fct/
        bridge/
        reports/
      dtm/
        hr/
        source_dwh_fdw.yml
    packages.yml
    profiles.yml
    seeds/
    snapshots/
    tests/
  great_expectations/         # Great Expectations config/context
  logs/
  plugins/
  py/                        # Python script độc lập
  requirements.txt
  scripts/                   # Shell script vận hành/backup/restore
    metabase_backup.sh
    restore_jira8db.sh
    warehouse_backup.sh
    start-airflow.sh
    stop-airflow.sh
  sql/                       # SQL thủ công: FDW, materialized view, test SQL
postgre/                      # PostgreSQL docker compose/init SQL
metabase/                     # BI service
minio/                        # S3-compatible storage
mysql/                        # MySQL source/config
jenkin/                       # Jenkins runtime (docker-compose, Dockerfile)
```

### 2.2 Khi bắt đầu task

Repository Analyst Agent nên kiểm tra:

- File tương tự trong `airflow/dags/` nếu task liên quan DAG.
- Factory trong `airflow/dags/factories/` nếu task liên quan pipeline.
- Config trong `airflow/dags/config/` nếu task liên quan source/target/dbt.
- Mapping trong `airflow/dags/utils/mappings.py` nếu task liên quan dbt model list.
- dbt model tương tự trong `airflow/dbt/models/` nếu task liên quan SQL transformation.

---

## 3. Airflow Orchestration Agent

**Mục tiêu:** thiết kế DAG, TaskGroup, dependency, schedule, retry, pool đúng pattern hiện có.

### 3.1 Convention Airflow hiện tại

- Dùng TaskFlow API với `@task`.
- Dùng `TaskGroup` cho group pipeline.
- Config chung nằm trong `airflow/dags/config/`.
- Timezone: `Asia/Ho_Chi_Minh`.
- Pool được map bằng `get_pool_name(table_type)`: `heavy` → `heavy_task_pool`, còn lại → `default_pool`.

Pattern import thường gặp:

```python
from __future__ import annotations
from airflow.sdk import TaskGroup
from airflow.decorators import task
```

### 3.2 Factory pattern cần ưu tiên

- `airflow/dags/factories/ingestion_factory.py`: extract/load source vào staging/DWH, có chunking, COPY insert, monitoring, quality check, notification.
- `airflow/dags/factories/dbt_factory.py`: chạy dbt transformation, success/failure logs, metrics, quality check, notification.
- `airflow/dags/factories/restore_factory.py`: restore/backup related tasks.

### 3.3 Flow chuẩn

```text
extract_load/dbt_run -> success_logs -> metrics -> quality_check -> notification
                    \-> failure_logs
```

### 3.4 Quy tắc khi thêm DAG/task

- Ưu tiên thêm config/mapping để factory tạo task thay vì viết DAG thủ công.
- Dùng return value của TaskFlow để truyền XCom.
- Khi tạo task trong loop, capture biến loop bằng default argument.
- Tránh build full task path để pull XCom thủ công nếu không cần.
- Không để import nặng ở top-level DAG nếu có thể đặt bên trong task.

---

## 4. Python ETL Engineer Agent

**Mục tiêu:** viết code Python extract/load/transform/helper rõ ràng, idempotent, chịu được data volume lớn.

### 4.1 Style

- Dùng Python 3 và type hints cho function mới nếu hợp lý.
- Tên biến rõ domain: `source_uri`, `target_schema`, `src_table`, `tgt_table`, `chunk_size`, `job_id`.
- Function public/helper quan trọng nên có docstring ngắn.
- Log bằng `print()` được chấp nhận trong Airflow task logs vì repo đang dùng pattern này.

### 4.2 Database access

- Dùng `sqlalchemy.create_engine` cho SQL database.
- Dispose engine trong `finally` nếu tạo engine trong task dài.
- Với PostgreSQL load lớn, ưu tiên COPY pattern trong `ingestion_factory.py`: `psql_insert_copy` và `df.to_sql(..., method=psql_insert_copy)`.
- Bảng lớn phải ưu tiên chunking/batch, không load toàn bộ vào memory nếu không cần.

### 4.3 Dynamic SQL safety

- Chỉ build SQL động từ schema/table lấy từ config nội bộ đáng tin cậy.
- Không đưa input user trực tiếp vào SQL string.
- Nếu cần value runtime, dùng bind parameter của SQLAlchemy.

---

## 5. dbt Transformation Agent

**Mục tiêu:** thiết kế transformation bằng dbt đúng layer, dễ maintain, có lineage bằng `ref()`/`source()`.

### 5.1 Project dbt

- dbt project nằm tại `airflow/dbt/`.
- Project name: `dwh_project`.
- Models chính dưới `models/dwh/` và `models/dtm/`.
- Target path: `target/{{ target.name }}`.

Materialization/schema hiện tại:

```yaml
models:
  dwh_project:
    dwh:
      intermediates:
        +schema: intermediates
        +materialized: table
      reports:
        +schema: reports
        +materialized: table
      dim:
        +schema: dim
        +materialized: table
      fct:
        +schema: fct
        +materialized: table
      bridge:
        +schema: bridge
        +materialized: table
```

### 5.2 Chọn đúng layer

- `models/dwh/intermediates/`: làm sạch staging/raw, rename/cast field, join nhẹ, chuẩn hóa dataset trước khi tạo dim/fct.
- `models/dwh/dim/`: dimension table, thuộc tính mô tả entity, có thể liên quan SCD/snapshot.
- `models/dwh/fct/`: fact table, chứa measure/metric, phải xác định grain rõ ràng.
- `models/dwh/bridge/`: bảng many-to-many.
- `models/dwh/reports/` hoặc `models/dtm/`: report/data mart/BI aggregate.

### 5.3 SQL style cho dbt

- Dùng CTE rõ ràng.
- Ưu tiên `{{ source(...) }}` cho source YAML.
- Ưu tiên `{{ ref(...) }}` để tham chiếu model dbt.
- Không dùng `select *` ở output cuối nếu schema cần ổn định.
- Đặt tên cột snake_case.
- Thêm `etl_datetime` nếu model hiện có cùng layer đang dùng tracking này.

Template nên dùng:

```sql
with source as (
    select *
    from {{ source('schema_name', 'table_name') }}
),

renamed as (
    select
        id,
        created_at,
        updated_at
    from source
)

select
    id,
    created_at,
    updated_at
from renamed
```

### 5.4 Tests/docs

Khi thêm model quan trọng, cân nhắc thêm file `.yml` cùng folder với test `not_null`, `unique`, relationship nếu phù hợp.

---

## 6. SQL / Data Warehouse Agent

**Mục tiêu:** đảm bảo SQL đúng grain, đúng naming, không tạo duplicate hoặc query quá nặng.

### 6.1 Naming convention

- Staging table: `stg_<source>_<table>` theo `get_target_table_name()`.
- Dimension: `dim_<entity>`.
- Fact: `fct_<business_process>`.
- Bridge: `bridge_<entity_a>_<entity_b>`.
- Intermediate: tên mô tả dataset/hành động, ví dụ `create_project`, `jira_issues`.
- Snapshot/SCD: dùng hậu tố như `_snapshot`, `_scd` hoặc theo pattern hiện có.

### 6.2 Grain/key rules

- Fact table phải có grain rõ trước khi viết SQL.
- Trước khi join 1-n, cần aggregate/deduplicate nếu không muốn nhân bản dòng.
- Dimension cần xác định natural key/business key.
- Bridge table cần xác định cặp key và uniqueness.

### 6.3 Performance rules

- Tránh cross join ngoài ý muốn.
- Filter sớm trong CTE nếu không làm sai logic.
- Chỉ select cột cần dùng.
- Với query/report nặng, cân nhắc materialized view hoặc index trong SQL riêng, nhưng không tự ý thêm nếu chưa rõ yêu cầu vận hành.

---

## 7. Data Quality & Monitoring Agent

**Mục tiêu:** giữ pipeline có log, metrics, quality check và notification nhất quán.

Pipeline đang dùng:

- `utils.monitoring.save_job_log`
- `utils.monitoring.save_metrics`
- `utils.monitoring.ETLMonitor`
- `utils.data_quality.validate_dataframe`
- `utils.data_quality_notification.send_validation_results`

Quy tắc:

- Không bỏ qua monitoring/quality nếu factory đã hỗ trợ.
- Nếu cần skip quality check, thêm vào danh sách skip có chủ đích và lý do rõ.
- Không thay đổi shape của `validation_result['results']` vì downstream notification phụ thuộc.
- Khi thêm metric mới, đảm bảo downstream SQL insert/logging không thiếu bind parameter.

---

## 8. DevOps / Runtime Agent

**Mục tiêu:** hỗ trợ chạy, kiểm tra, debug môi trường Docker Compose/Airflow/dbt.

### 8.1 Lệnh chạy stack

```bash
cd airflow
docker compose up airflow-init
docker compose up -d --build --scale airflow-worker=3
```

### 8.2 Lệnh dừng stack

```bash
cd airflow
docker compose down --remove-orphans
```

### 8.3 Lệnh dbt trong container Airflow

```bash
cd /opt/airflow/dbt
dbt deps --profiles-dir /opt/airflow/dbt
dbt run --profiles-dir /opt/airflow/dbt --select <model_name>
dbt test --profiles-dir /opt/airflow/dbt --select <model_name>
```

### 8.4 Great Expectations init

```bash
docker exec -it airflow-airflow-worker-1 /bin/bash
cd great_expectations
python3 -c "from great_expectations.data_context import get_context; get_context()"
```

### 8.5 Dependency rule

Nếu thêm dependency Python:

1. Cập nhật `airflow/requirements.txt`.
2. Giải thích lý do cần dependency.
3. Kiểm tra dependency có tương thích Airflow/dbt hiện có không.

---

## 9. Reviewer / Safety Agent

**Mục tiêu:** review cuối trước khi kết luận task hoàn thành.

### 9.1 Checklist bắt buộc

- [ ] File được đặt đúng layer/thư mục.
- [ ] Không hard-code secret/token/password/host nhạy cảm.
- [ ] Không đổi tên DAG/task/schema/table/mapping production nếu không được yêu cầu.
- [ ] DAG parse được về mặt syntax/import.
- [ ] Task id, DAG id, TaskGroup id không trùng bất thường.
- [ ] dbt model dùng `ref()`/`source()` đúng khi phù hợp.
- [ ] SQL có grain rõ, tránh duplicate ngoài ý muốn.
- [ ] Bảng lớn dùng chunking/pool/batch/COPY hợp lý.
- [ ] Có logging/metrics/quality check nếu pipeline yêu cầu.
- [ ] Không làm vỡ shape dữ liệu downstream, đặc biệt `validation_result['results']`.
- [ ] Nếu thêm dependency, đã cập nhật `airflow/requirements.txt`.

### 9.2 Khi không chắc chắn

Nếu chưa rõ requirement hoặc domain business:

1. Hỏi lại user trước khi sửa schema/model quan trọng.
2. Đọc model/DAG/factory tương tự trước khi viết mới.
3. Ghi rõ giả định trong câu trả lời hoặc comment nếu cần.
4. Ưu tiên thay đổi nhỏ, dễ rollback.

---

## 10. Quy trình phối hợp giữa các agent

Khi nhận một yêu cầu mới, AI nên đi theo flow:

```text
1. Chief ETL Architect Agent
   -> phân loại task, xác định phạm vi và rủi ro

2. Repository Analyst Agent
   -> đọc file/pattern liên quan

3. Agent chuyên trách
   -> Airflow / Python / dbt / SQL / Data Quality / DevOps thực hiện thay đổi

4. Reviewer / Safety Agent
   -> kiểm tra checklist, syntax, convention, backward compatibility

5. Chief ETL Architect Agent
   -> tóm tắt thay đổi, nêu cách test/chạy nếu cần
```

### 10.1 Mapping task sang agent

| Loại task | Agent chính | Agent phối hợp |
|---|---|---|
| Thêm source/table ingestion | Airflow Orchestration Agent | Python ETL Engineer, Data Quality |
| Sửa extract/load logic | Python ETL Engineer Agent | Airflow Orchestration, Reviewer |
| Thêm dbt model dim/fct | dbt Transformation Agent | SQL/DWH, Data Quality |
| Tối ưu SQL/report | SQL / Data Warehouse Agent | dbt Transformation, Reviewer |
| Thêm quality check/notification | Data Quality & Monitoring Agent | Airflow Orchestration |
| Sửa Docker/requirements/script | DevOps / Runtime Agent | Reviewer |
| Refactor lớn | Chief ETL Architect Agent | Tất cả agent liên quan |

---

## 11. Nguyên tắc ưu tiên khi code

1. **Đúng kiến trúc hiện có trước, tối ưu sau.**
2. **Config/mapping trước, custom code sau.**
3. **TaskFlow return value trước, XCom thủ công sau.**
4. **`ref()`/`source()` trước, hard-code relation sau.**
5. **Chunking/COPY trước, full-load in-memory sau.**
6. **Backward compatible trước, rename/breaking change sau.**
7. **Hỏi lại khi chưa chắc, không đoán schema nghiệp vụ quan trọng.**
