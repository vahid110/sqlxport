#!/bin/bash
set -e

INPUT_PATH="s3a://demo-bucket/sales/sales.parquet"
OUTPUT_PATH="delta_output"
PARTITIONED_FLAG=""

# Accept optional arguments
if [[ "$1" != "" ]]; then
  INPUT_PATH="$1"
fi

if [[ "$2" == "--partitioned" ]]; then
  PARTITIONED_FLAG="--partitioned"
  shift
fi

if [[ "$2" != "" ]]; then
  OUTPUT_PATH="$2"
fi

echo "🔁 Rebuilding Spark image (if needed)..."
docker compose build spark-runner

echo "🚀 Running Spark Delta conversion inside Docker..."
docker compose exec spark-runner /opt/bitnami/spark/bin/spark-submit \
  --master local[*] \
  --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
  --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
  --packages io.delta:delta-core_2.12:2.4.0 \
  run_spark_query.py "$INPUT_PATH" $PARTITIONED_FLAG --output-path "$OUTPUT_PATH"
