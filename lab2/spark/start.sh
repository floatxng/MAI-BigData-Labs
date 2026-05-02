#!/bin/bash
set -e

sleep 30
spark-submit --jars /opt/spark/extra-jars/clickhouse4j-1.4.4.jar /opt/spark/apps/pipeline.py
tail -f /dev/null
