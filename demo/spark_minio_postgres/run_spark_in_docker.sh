#!/bin/bash
set -e

echo "🔁 Rebuilding Spark image (if needed)..."
docker compose build spark-runner

echo "🚀 Running Spark Delta conversion inside Docker..."
docker compose exec spark-runner /opt/bitnami/spark/bin/spark-submit \
  --master local[*] \
  --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
  --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
  --packages io.delta:delta-core_2.12:2.4.0 \
  run_spark_query.py
