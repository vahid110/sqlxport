#!/bin/bash
set -euo pipefail

echo -e "\n🔁 \033[1;34mRebuilding Spark Docker image (if needed)...\033[0m"
docker compose build spark-runner

echo -e "\n🚀 \033[1;32mRunning Spark script inside Docker...\033[0m"
docker compose exec spark-runner /opt/bitnami/spark/bin/spark-submit \
  --master local[*] \
  --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
  --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
  run_spark_query.py
