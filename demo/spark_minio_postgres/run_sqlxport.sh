#!/bin/bash
set -e

echo "🧹 Step 0: Cleaning up any previous demo artifacts..."
rm -f sales.parquet
rm -rf delta_output sales_delta sales_partitioned_delta
docker rm -f demo-db-spark-minio-postgres demo-minio >/dev/null 2>&1 || true
docker volume rm -f demo_minio-data spark_delta_pgdata >/dev/null 2>&1 || true

# Optional: Reset PostgreSQL demo DB if it exists
if docker compose ps | grep -q demo-db-spark-minio-postgres; then
  echo "🧺 Dropping existing demo DB (if any)..."
  docker compose exec demo-db-spark-minio-postgres psql -U postgres -c "DROP DATABASE IF EXISTS demo;"
fi

echo "🚀 Step 1: Starting MinIO + PostgreSQL + Spark Delta Lake demo..."

# Start Docker containers
echo "🧱 Starting services..."
docker compose up -d

# Wait for PostgreSQL to become ready
echo "⏳ Waiting for PostgreSQL to be ready..."
sleep 10

# Seed the PostgreSQL database
echo "🌱 Seeding PostgreSQL..."
docker compose exec demo-db-spark-minio-postgres psql -U postgres -tc "SELECT 1 FROM pg_database WHERE datname = 'demo'" | grep -q 1 || \
  docker compose exec demo-db-spark-minio-postgres psql -U postgres -c "CREATE DATABASE demo;"
docker compose exec demo-db-spark-minio-postgres psql -U postgres -d demo -c "CREATE TABLE IF NOT EXISTS sales (id SERIAL PRIMARY KEY, region TEXT, amount NUMERIC);"
docker compose exec demo-db-spark-minio-postgres psql -U postgres -d demo -c "INSERT INTO sales (region, amount) SELECT * FROM (VALUES ('EMEA', 100), ('NA', 200), ('APAC', 150)) AS tmp(region, amount);"

# Create MinIO bucket (if not exists)
echo "🪣 Creating bucket if not exists..."
docker run --rm --network spark_minio_postgres_default \
  -e MC_HOST_local=http://minioadmin:minioadmin@minio:9000 \
  minio/mc mb -q --ignore-existing local/demo-bucket

# Step 2: Export data to Parquet and upload to MinIO
echo "📤 Step 2: Exporting sales table to Parquet in MinIO with sqlxport..."
sqlxport export \
  --db-url postgresql://postgres:postgres@localhost:5432/demo \
  --query "SELECT * FROM sales" \
  --output-file sales.parquet \
  --format parquet \
  --export-mode postgres-query \
  --s3-bucket demo-bucket \
  --s3-key sales/sales.parquet \
  --s3-endpoint http://localhost:9000 \
  --s3-access-key minioadmin \
  --s3-secret-key minioadmin \
  --aws-region us-east-1

# Step 3: Convert to Delta via Spark
echo "✨ Step 3: Launching Spark job to convert Parquet to Delta format..."
./run_spark_in_docker.sh "s3a://demo-bucket/sales/sales.parquet" "" "sales_delta"

# Step 4: Preview result using DuckDB (fallback)
echo "🔍 Step 4: Verifying Delta output via DuckDB preview..."
duckdb -c "SELECT * FROM 'delta_output/*.parquet' LIMIT 10"

./verify_outputs.sh delta_output

# ──────────────────────────────────────────────────────────────
# Additional bulk variants from run_sqlxport_bulk.sh
# ──────────────────────────────────────────────────────────────

echo
echo "🧪 Step 5: [Bulk Demo] Running --partitioned (default output dir)..."
./run_sqlxport_bulk.sh --partitioned

echo
echo "🧪 Step 6: [Next Bulk Demo] Running --output-dir sales_partitioned_delta (no partitioning)..."
./run_sqlxport_bulk.sh --output-dir sales_partitioned_delta

echo
echo "🧪 Step 7: [Next Bulk Demo] Running --partitioned --output-dir sales_partitioned_delta..."
./run_sqlxport_bulk.sh --partitioned --output-dir sales_partitioned_delta

echo
echo "✅ All demo steps completed successfully."
