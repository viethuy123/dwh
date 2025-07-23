#!/bin/bash

CONTAINER_NAME="postgre-dwh_postgres-1"  
POSTGRES_USER="dev_user"            
MOUNTED_BACKUP_DIR="/opt/airflow/database_backup/warehouse"  
CONTAINER_BACKUP_DIR="/tmp"        
BACKUP_DATE=$(date +%Y%m%d)
BACKUP_FILE="warehouse_databases_${BACKUP_DATE}.sql"
BACKUP_LOG="${MOUNTED_BACKUP_DIR}/warehouse_backup_${BACKUP_DATE}.log"

exec > >(tee -a "$BACKUP_LOG") 2>&1

echo "Starting PostgreSQL daily backup process at $(date)"

if [ -f "${MOUNTED_BACKUP_DIR}/${BACKUP_FILE}" ] || [ -f "${MOUNTED_BACKUP_DIR}/${BACKUP_FILE}.gz" ]; then
    echo "Backup for $BACKUP_DATE already exists in local directory, skipping backup"
    exit 0
fi

pg_dumpall -h "$CONTAINER_NAME" -U "$POSTGRES_USER" --schema-only -f "${CONTAINER_BACKUP_DIR}/${BACKUP_FILE}"

if [ $? -eq 0 ]; then
    echo "Successfully created backup in container: ${CONTAINER_BACKUP_DIR}/${BACKUP_FILE}"
    
    cp "${CONTAINER_BACKUP_DIR}/${BACKUP_FILE}" "${MOUNTED_BACKUP_DIR}/${BACKUP_FILE}"
    
    if [ $? -eq 0 ]; then
        echo "Successfully copied backup to local: ${MOUNTED_BACKUP_DIR}/${BACKUP_FILE}"
        
        gzip "${MOUNTED_BACKUP_DIR}/${BACKUP_FILE}"
        if [ $? -eq 0 ]; then
            echo "Successfully compressed ${MOUNTED_BACKUP_DIR}/${BACKUP_FILE}.gz"
        else
            echo "Error: Failed to compress ${MOUNTED_BACKUP_DIR}/${BACKUP_FILE}"
        fi
        
        rm "${CONTAINER_BACKUP_DIR}/${BACKUP_FILE}"
        if [ $? -eq 0 ]; then
            echo "Successfully removed temporary backup file from container"
        else
            echo "Warning: Failed to remove temporary backup file from container"
        fi
    else
        echo "Error: Failed to copy backup from container to local directory"
        exit 1
    fi
else
    echo "Error: Failed to create backup in container"
    exit 1
fi

find "$MOUNTED_BACKUP_DIR" -name "warehouse_databases_*.sql.gz" -mtime +2 -delete

find "$MOUNTED_BACKUP_DIR" -name "warehouse_backup_*.log" -mtime +2 -delete

echo "Backup process completed at $(date)"

exit 0