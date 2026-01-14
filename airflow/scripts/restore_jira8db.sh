#!/bin/bash
set -euo pipefail

MYSQL_OPTS="-h dwh_mysql -u root"
TARGET_DIR="/opt/airflow/database_backup/${DB_NAME}_bk_${YESTERDAY_STR}_22"
SQL_FILE=$(find "$TARGET_DIR" -name "*.sql" | head -n 1)

if [[ -z "$SQL_FILE" || ! -f "$SQL_FILE" ]]; then
    echo "[ERROR] No SQL file found"
    exit 1
fi

echo "[1/3] Turning off safety checks (Dynamic variables only)... ${YESTERDAY_STR}"
# Bỏ innodb_doublewrite vì nó yêu cầu restart server
mysql $MYSQL_OPTS -e "
  SET GLOBAL innodb_flush_log_at_trx_commit = 0;
  SET GLOBAL sync_binlog = 0;
"

echo "[2/3] Restoring database ${DB_NAME}..."
# Tối ưu ở mức Session (chỉ có tác dụng trong lần kết nối này)
(
  echo "SET SESSION FOREIGN_KEY_CHECKS=0;"
  echo "SET SESSION UNIQUE_CHECKS=0;"
  echo "SET SESSION AUTOCOMMIT=0;"
  cat "$SQL_FILE"
  echo "COMMIT;"
) | mysql $MYSQL_OPTS "${DB_NAME}"

echo "[3/3] Restoring safety settings..."
mysql $MYSQL_OPTS -e "
  SET GLOBAL innodb_flush_log_at_trx_commit = 1;
  SET GLOBAL sync_binlog = 1;
"

# Kiểm tra kết quả
RESULT=$(mysql $MYSQL_OPTS -N -e "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='${DB_NAME}';")
echo "[SUCCESS] Restore finished. Total tables: ${RESULT}"