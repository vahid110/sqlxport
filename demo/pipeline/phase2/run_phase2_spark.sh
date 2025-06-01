#!/bin/bash
set -e

# ------------------------------
# Pipeline Phase 2: Spark Transform
# ------------------------------

# Config
INPUT_S3_PATH="s3a://demo-bucket/sales_data/"
OUTPUT_S3_PATH="s3a://demo-bucket/sales_transformed/"
OUTPUT_FORMAT="parquet"  # or "csv"

# These environment variables are assumed to be set by docker-compose or your environment
export AWS_ACCESS_KEY_ID=minioadmin
export AWS_SECRET_ACCESS_KEY=minioadmin
export AWS_REGION=us-east-1
export S3_ENDPOINT=http://minio:9000

echo "🧪 Running Spark job to process data from: $INPUT_S3_PATH"
docker exec spark_minio_postgres-spark-runner-1 spark-submit \
  --master local[*] \
  /opt/spark/jobs/spark_transform.py \
  --input "$INPUT_S3_PATH" \
  --output "$OUTPUT_S3_PATH" \
  --format "$OUTPUT_FORMAT"

echo "✅ Phase 2 Spark transformation complete. Output written to: $OUTPUT_S3_PATH"
