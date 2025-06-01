#!/bin/bash
set -e

echo "🚀 Starting MinIO + PostgreSQL + Spark Delta Lake demo..."

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

echo "🪣 Creating bucket if not exists..."
docker run --rm --network spark_minio_postgres_default \
  -e MC_HOST_local=http://minioadmin:minioadmin@minio:9000 \
  minio/mc mb -q --ignore-existing local/demo-bucket

# Export data to Parquet and upload to MinIO
echo "📤 Exporting sales table to Parquet in MinIO with sql2data..."
sql2data run \
  --db-url postgresql://postgres:postgres@localhost:5432/demo \
  --query "SELECT * FROM sales" \
  --output-file sales.parquet \
  --format parquet \
  --s3-bucket demo-bucket \
  --s3-key sales/sales.parquet \
  --s3-endpoint http://localhost:9000 \
  --s3-access-key minioadmin \
  --s3-secret-key minioadmin \
  --aws-region us-east-1

# Launch Spark job to process from MinIO
echo "✨ Launching Spark job to convert Parquet to Delta format..."

# New...
./run_spark_in_docker.sh "s3a://demo-bucket/sales/sales.parquet" "" "sales_delta"


# Verify
echo "🔍 Verifying Delta output via fallback preview (DuckDB can't read Delta metadata)..."
duckdb -c "SELECT * FROM 'delta_output/*.parquet' LIMIT 10"


# Verify
echo "🔍 Verifying Delta output via fallback preview (DuckDB can't read Delta metadata)..."
./verify_outputs.sh sales_delta

echo "✅ Demo complete."
