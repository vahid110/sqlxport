#!/bin/bash
set -e

echo "📦 Preparing Spark + Delta Lake demo using Docker..."

# Start Docker services
echo "🐘 Starting PostgreSQL and Spark via Docker Compose..."
docker compose up -d

# Wait for PostgreSQL to initialize
echo "⏳ Waiting for PostgreSQL to be ready..."
until docker exec demo-db-spark-delta pg_isready -U postgres &>/dev/null; do
  sleep 1
done

# Seed the demo database (idempotent)
echo "🌱 Seeding database..."
docker exec demo-db-spark-delta psql -U postgres -tc "SELECT 1 FROM pg_database WHERE datname = 'demo'" | grep -q 1 ||   docker exec demo-db-spark-delta psql -U postgres -c "CREATE DATABASE demo;"

docker exec demo-db-spark-delta psql -U postgres -d demo -c "CREATE TABLE IF NOT EXISTS sales (id SERIAL PRIMARY KEY, region TEXT, amount NUMERIC);"
docker exec demo-db-spark-delta psql -U postgres -d demo -c "INSERT INTO sales (region, amount) SELECT * FROM (VALUES ('EMEA', 100), ('NA', 200), ('APAC', 150)) AS tmp(region, amount) ON CONFLICT DO NOTHING;"

# Export data to Parquet
echo "📤 Exporting sales table to Parquet with sql2data..."
sql2data run \
  --db-url postgresql://postgres:postgres@localhost:5432/demo \
  --query "SELECT * FROM sales" \
  --format parquet \
  --output-file sales.parquet

# Run Spark job to convert to Delta and query
echo "✨ Running Spark transformation and query..."
./run_spark_in_docker.sh

# Verification
echo -e "\n\033[1;34m🔍 Verifying exported Parquet data (non-Delta)...\033[0m"
if command -v duckdb &>/dev/null; then
  duckdb -c "SELECT * FROM 'sales.parquet' LIMIT 10"
else
  echo "⚠️ DuckDB not found. Skipping Parquet preview."
fi

echo -e "\n\033[1;34m🔍 Verifying Delta output via Parquet fallback (DuckDB doesn't support Delta metadata)...\033[0m"
if command -v duckdb &>/dev/null; then
  duckdb -c "SELECT * FROM 'delta_output/*.parquet' LIMIT 10"
else
  echo "⚠️ DuckDB not found. Skipping Delta fallback preview."
fi

echo -e "\n✅ \033[1;32mDone.\033[0m"
