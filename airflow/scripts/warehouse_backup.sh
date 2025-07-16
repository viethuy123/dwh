#!/bin/bash

CONTAINER_NAME="postgre-dwh_postgres-1"  
POSTGRES_USER="dev_user"            
LOCAL_BACKUP_DIR="/home/user/warehouse/airflow/database_backup/warehouse"  
CONTAINER_BACKUP_DIR="/tmp"        
BACKUP_DATE=$(date +%Y%m%d)
BACKUP_FILE="warehouse_databases_${BACKUP_DATE}.sql"
BACKUP_LOG="${LOCAL_BACKUP_DIR}/warehouse_backup_${BACKUP_DATE}.log"

exec > >(tee -a "$BACKUP_LOG") 2>&1

echo "Starting PostgreSQL daily backup process at $(date)"

if [ -f "${LOCAL_BACKUP_DIR}/${BACKUP_FILE}" ] || [ -f "${LOCAL_BACKUP_DIR}/${BACKUP_FILE}.gz" ]; then
    echo "Backup for $BACKUP_DATE already exists in local directory, skipping backup"
    exit 0
fi

docker exec "$CONTAINER_NAME" pg_dumpall -U "$POSTGRES_USER" --schema-only -f "${CONTAINER_BACKUP_DIR}/${BACKUP_FILE}"

if [ $? -eq 0 ]; then
    echo "Successfully created backup in container: ${CONTAINER_BACKUP_DIR}/${BACKUP_FILE}"
    
    docker cp "${CONTAINER_NAME}:${CONTAINER_BACKUP_DIR}/${BACKUP_FILE}" "${LOCAL_BACKUP_DIR}/${BACKUP_FILE}"
    
    if [ $? -eq 0 ]; then
        echo "Successfully copied backup to local: ${LOCAL_BACKUP_DIR}/${BACKUP_FILE}"
        
        gzip "${LOCAL_BACKUP_DIR}/${BACKUP_FILE}"
        if [ $? -eq 0 ]; then
            echo "Successfully compressed ${LOCAL_BACKUP_DIR}/${BACKUP_FILE}.gz"
        else
            echo "Error: Failed to compress ${LOCAL_BACKUP_DIR}/${BACKUP_FILE}"
        fi
        
        docker exec "$CONTAINER_NAME" rm "${CONTAINER_BACKUP_DIR}/${BACKUP_FILE}"
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

find "$LOCAL_BACKUP_DIR" -name "warehouse_databases_*.sql.gz" -mtime +7 -delete

echo "Backup process completed at $(date)"

chmod -R 600 "$LOCAL_BACKUP_DIR"

exit 0