#!/bin/bash
set -euo pipefail

echo -e "\n🔹 [1/7] Starting services..."
docker compose up -d

echo -e "\n🔹 [2/7] Waiting for PostgreSQL to be ready..."
until docker compose exec postgres pg_isready -U postgres &>/dev/null; do
  sleep 1
done

echo -e "\n🔹 [3/7] Seeding PostgreSQL..."
docker compose exec postgres psql -U postgres -tc "SELECT 1 FROM pg_database WHERE datname = 'demo'" | grep -q 1 || \
  docker compose exec postgres psql -U postgres -c "CREATE DATABASE demo;"

docker compose exec postgres psql -U postgres -d demo -c "CREATE TABLE IF NOT EXISTS logs (id SERIAL PRIMARY KEY, service TEXT, ts TIMESTAMP);"
docker compose exec postgres psql -U postgres -d demo -c "INSERT INTO logs (service, ts) SELECT 'web', now() - i * interval '1 hour' FROM generate_series(1, 10) i;"

echo -e "\n🔹 [4/7] Exporting logs table to Parquet with partitioning..."
sqlxport export \
  --db-url "postgresql://postgres:password@localhost:5433/demo" \
  --query "SELECT * FROM logs" \
  --format parquet \
  --output-dir logs_partitioned \
  --partition-by service \
  --s3-provider minio \
  --s3-endpoint http://localhost:9002 \
  --s3-access-key minioadmin \
  --s3-secret-key minioadmin \
  --s3-bucket athena-demo \
  --athena-s3-prefix logs/

echo -e "\n🔹 [5/7] Previewing partitioned files with DuckDB (Athena-simulation)..."
duckdb -c "INSTALL parquet; LOAD parquet;
SELECT service, COUNT(*) AS count
FROM 'logs_partitioned/service=*/**/*.parquet'
GROUP BY service;"

echo -e "\n🔹 [6/7] Registering with Glue Catalog"
./glue_register.sh

echo -e "\n🔹 [7/7] Demo complete."
