#!/bin/bash
set -eu

# ------------------------------
# Unified Pipeline: Phase 1 + Phase 2
# ------------------------------

# Check for docker-compose
if ! command -v docker-compose >/dev/null 2>&1; then
  echo "❌ docker-compose not found. Please install it first."
  exit 1
fi

# Start services if not running
if ! docker ps --format '{{.Names}}' | grep -q '^spark-runner$'; then
  echo "🚀 Starting required services via docker-compose..."
  docker-compose up -d

  echo "⏳ Waiting for PostgreSQL to be ready..."
  until docker exec demo-db-pipeline pg_isready -U postgres > /dev/null 2>&1; do
    sleep 1
  done
  echo "✅ PostgreSQL is ready."
else
  echo "✅ Services already running."
fi


# Defaults
PARTITIONED=0
FORMAT="parquet"
OUTPUT_DIR="pipeline_output"
USE_S3=0

# Parse CLI args
while [[ "$#" -gt 0 ]]; do
  case $1 in
    --partitioned) PARTITIONED=1 ;;
    --format) FORMAT="$2"; shift ;;
    --output-dir) OUTPUT_DIR="$2"; shift ;;
    --use-s3) USE_S3=1 ;;
    *) echo "❌ Unknown parameter: $1"; exit 1 ;;
  esac
  shift
done

# ------------------------------
# Phase 1: Extract + Upload to S3
# ------------------------------
echo "📤 Running Phase 1 (Extract & Upload)..."
./run_pipeline.sh \
  ${PARTITIONED:+--partitioned} \
  --format "$FORMAT" \
  --output-dir "$OUTPUT_DIR" \
  ${USE_S3:+--use-s3}

# ------------------------------
# Phase 2: Spark Transform
# ------------------------------
echo "🔥 Starting Phase 2 (Spark transform)..."
docker exec spark-runner \
  spark-submit /opt/spark_job/spark_transform.py

echo "✅ Full pipeline completed successfully."
