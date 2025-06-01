#!/bin/bash
set -e

PARTITIONED=0
OUTPUT_DIR="sales_delta"

# Parse arguments
while [[ "$#" -gt 0 ]]; do
  case $1 in
    --partitioned) PARTITIONED=1 ;;
    --output-dir) OUTPUT_DIR="$2"; shift ;;
    *) echo "Unknown parameter passed: $1"; exit 1 ;;
  esac
  shift
done

echo "🚀 Starting MinIO + PostgreSQL + Spark Delta Lake BULK demo..."

# Start Docker containers
echo "🧱 Starting services..."
docker compose up -d

# Wait for PostgreSQL to become ready
echo "⏳ Waiting for PostgreSQL to be ready..."
sleep 10

# Seed the PostgreSQL database with 3x1M rows
echo "🌋 Seeding PostgreSQL with 1M rows..."
docker compose exec demo-db psql -U postgres -tc "SELECT 1 FROM pg_database WHERE datname = 'demo'" | grep -q 1 || \
  docker compose exec demo-db psql -U postgres -c "CREATE DATABASE demo;"
docker compose exec demo-db psql -U postgres -d demo -c "CREATE TABLE IF NOT EXISTS sales (id SERIAL PRIMARY KEY, region TEXT, amount NUMERIC);"
docker compose exec demo-db psql -U postgres -d demo -c "TRUNCATE TABLE sales;"
docker compose exec demo-db psql -U postgres -d demo -c "
  INSERT INTO sales (region, amount)
  SELECT region, ROUND((random() * 1000)::numeric, 2)
  FROM generate_series(1, 1000000),
       (VALUES ('EMEA'), ('NA'), ('APAC')) AS r(region);
"

# Create the MinIO bucket
echo "🪣 Creating bucket if not exists..."
docker run --rm --network spark_minio_postgres_default \
  -e MC_HOST_local=http://minioadmin:minioadmin@minio:9000 \
  minio/mc mb -q --ignore-existing local/demo-bucket

# Export data to Parquet and upload to MinIO using sql2data
echo "📤 Exporting sales table to Parquet in MinIO with sql2data..."

if [[ $PARTITIONED -eq 1 ]]; then
  echo "📦 Exporting in partitioned mode by region..."
  sql2data run \
    --db-url postgresql://postgres:postgres@localhost:5432/demo \
    --query "SELECT * FROM sales" \
    --output-dir "$OUTPUT_DIR" \
    --format parquet \
    --partition-by region \
    --s3-bucket demo-bucket \
    --s3-key "$OUTPUT_DIR/" \
    --s3-endpoint http://localhost:9000 \
    --s3-access-key minioadmin \
    --s3-secret-key minioadmin \
    --aws-region us-east-1
else
  echo "📦 Exporting in flat (non-partitioned) mode..."
  sql2data run \
    --db-url postgresql://postgres:postgres@localhost:5432/demo \
    --query "SELECT * FROM sales" \
    --output-file "$OUTPUT_DIR.parquet" \
    --format parquet \
    --s3-bucket demo-bucket \
    --s3-key "$OUTPUT_DIR/$OUTPUT_DIR.parquet" \
    --s3-endpoint http://localhost:9000 \
    --s3-access-key minioadmin \
    --s3-secret-key minioadmin \
    --aws-region us-east-1
fi

# Launch Spark job to process from MinIO
echo "✨ Launching Spark job to convert Parquet to Delta format..."
PARTITION_FLAG=""
if [[ $PARTITIONED -eq 1 ]]; then
  PARTITION_FLAG="--partitioned"
fi

./run_spark_in_docker.sh "s3a://demo-bucket/$OUTPUT_DIR" $PARTITION_FLAG "$OUTPUT_DIR"

# Verify output with DuckDB fallback
echo "🔍 Verifying Delta output via fallback preview (DuckDB can't read Delta metadata)..."
duckdb -c "SELECT COUNT(*) FROM '${OUTPUT_DIR}/**/*.parquet'"

echo "✅ Bulk demo complete."
