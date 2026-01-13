#!/bin/bash
set -euo pipefail

# YESTERDAY_STR được Airflow truyền qua env (ví dụ: 20260112)
TARGET_DIR="/opt/airflow/database_backup/${DB_NAME}_bk_${YESTERDAY_STR}_22"

echo "[INFO] Searching for SQL file in: ${TARGET_DIR}"

# Tìm file .sql đầu tiên trong thư mục giải nén
SQL_FILE=$(find "$TARGET_DIR" -name "*.sql" | head -n 1)

if [[ -z "$SQL_FILE" || ! -f "$SQL_FILE" ]]; then
    echo "[ERROR] No SQL file found in ${TARGET_DIR}"
    exit 1
fi

echo "[INFO] Restoring database ${DB_NAME} from ${SQL_FILE}..."
mysql -h dwh_mysql -u root "${DB_NAME}" < "${SQL_FILE}"

# Xác thực sơ bộ
RESULT=$(mysql -N -u root -e "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='${DB_NAME}';")
echo "[SUCCESS] Restore finished. Total tables in ${DB_NAME}: ${RESULT}"