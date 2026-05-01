#!/bin/bash
set -e

echo "Ожидание готовности сервисов..."
sleep 30

echo "=== Запуск ETL-пайплайна (PostgreSQL -> Star Schema -> ClickHouse Marts) ==="
spark-submit --jars /opt/spark/extra-jars/clickhouse4j-1.4.4.jar /opt/spark/apps/pipeline.py

echo "=== ETL завершён ==="
tail -f /dev/null
