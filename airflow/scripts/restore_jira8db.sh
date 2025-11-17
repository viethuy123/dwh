#!/bin/bash
set -euo pipefail

YESTERDAY_STR=$(date -d "yesterday" +"%Y%m%d")
BACKUP_FILE="/opt/airflow/database_backup/jira8db_bk_${YESTERDAY_STR}_22/hdd2/jiradb/backup/jira8db_bk_${YESTERDAY_STR}_22.sql"
REMOTE_HOST="dwh_mysql"
REMOTE_PORT=3306
REMOTE_MYSQL_USER="root"
REMOTE_DB="jira8db"

echo "[INFO] Restoring backup into MySQL on Remote Server..."
mysql -h ${REMOTE_HOST} \
      -P"${REMOTE_PORT}" \
      -u${REMOTE_MYSQL_USER} \
      ${REMOTE_DB} < "${BACKUP_FILE}"


echo "[INFO] Verifying restore on Remote Server..."
ROW_COUNT=$(mysql -N -h ${REMOTE_HOST} \
      -P"${REMOTE_PORT}" \
      -u${REMOTE_MYSQL_USER} \
      -e "SELECT SUM(TABLE_ROWS) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='${REMOTE_DB}';")

if [[ -z "$ROW_COUNT" || "$ROW_COUNT" == "NULL" || "$ROW_COUNT" -eq 0 ]]; then
    echo "[ERROR] Restore may have failed. Row count is zero or NULL."
    exit 1
else
    echo "[SUCCESS] Restore appears successful. Total rows: $ROW_COUNT"
fi
