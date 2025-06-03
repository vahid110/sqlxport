#!/bin/bash
# demo/spark_minio_delta/run_sqlxport.sh

set -ex

echo -e "\n📦 Preparing Spark + Delta Lake on MinIO demo..."

echo -e "\n🐘 Starting PostgreSQL and MinIO via Docker Compose..."
docker compose up -d

echo -e "\n⏳ Waiting for PostgreSQL to be ready..."
until docker compose exec demo-db pg_isready -U postgres &>/dev/null; do
  sleep 1
done

echo -e "\n🌱 Seeding PostgreSQL with demo data..."
docker compose exec demo-db psql -U postgres -tc "SELECT 1 FROM pg_database WHERE datname = 'demo'" | grep -q 1 || \
  docker compose exec demo-db psql -U postgres -c "CREATE DATABASE demo;"

docker compose exec demo-db psql -U postgres -d demo -c "CREATE TABLE IF NOT EXISTS sales (id SERIAL PRIMARY KEY, region TEXT, amount NUMERIC);"
docker compose exec demo-db psql -U postgres -d demo -c "INSERT INTO sales (region, amount) SELECT * FROM (VALUES ('EMEA', 100), ('NA', 200), ('APAC', 150)) AS tmp(region, amount) ON CONFLICT DO NOTHING;"

echo -e "\n📤 Exporting sales table to Parquet with sqlxport..."
sqlxport run \
  --db-url "postgresql://postgres:password@localhost:5432/demo" \
  --query "SELECT * FROM sales" \
  --format parquet \
  --output-file sales.parquet

echo "💾 Parquet file saved as sales.parquet"

echo -e "\n🪣 Ensuring MinIO 'demo' bucket exists..."
mc alias set local http://localhost:9000 minioadmin minioadmin &>/dev/null || true
mc mb -q --ignore-existing local/demo

echo -e "\n✨ Running Spark job with Delta Lake support..."
docker compose exec spark-runner spark-submit \
  --conf spark.hadoop.hadoop.security.authentication=Simple \
  --conf spark.driver.extraJavaOptions="-Djava.security.krb5.conf=/dev/null" \
  --conf spark.executor.extraJavaOptions="-Djava.security.krb5.conf=/dev/null" \
  --conf spark.hadoop.fs.s3a.access.key=minioadmin \
  --conf spark.hadoop.fs.s3a.secret.key=minioadmin \
  --conf spark.hadoop.fs.s3a.endpoint=http://demo-minio:9000 \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --jars /tmp/jars/delta-core_2.12-2.1.0.jar,/tmp/jars/delta-storage-2.1.0.jar \
  /app/run_spark_query.py

echo -e "\n🔍 Verifying MinIO outputs..."
if command -v mc &>/dev/null; then
  echo -e "\n📁 Buckets in MinIO:"
  mc ls local/

  echo -e "\n📂 Contents of 'demo/' bucket:"
  mc ls local/demo/

  echo -e "\n📄 CSV files in 'demo/csv_output/':"
  mc ls local/demo/csv_output/ || echo "❌ No CSV output found."

  echo -e "\n📄 Delta partition folders in 'demo/delta_partitioned/':"
  mc ls local/demo/delta_partitioned/ || echo "❌ No Delta output found."

  echo -e "\n🔎 Preview of Delta structure:"
  mc ls --recursive local/demo/delta_partitioned/ | head -n 10

  echo -e "\n⬇️ Copying Delta data from MinIO to local disk (for DuckDB preview)..."
  mkdir -p tmp_delta_partitioned
  mc cp --recursive local/demo/delta_partitioned/ tmp_delta_partitioned/
else
  echo "⚠️ MinIO Client (mc) not installed. Skipping verification steps."
fi

echo -e "\n🦆 Querying Delta files using DuckDB..."
if command -v duckdb &>/dev/null && [ -d "tmp_delta_partitioned/region=EMEA" ]; then
  duckdb -c "INSTALL parquet; LOAD parquet;
             SELECT region, COUNT(*) AS count
             FROM 'tmp_delta_partitioned/region=EMEA/*.parquet'
             GROUP BY region;"
else
  echo "❌ DuckDB or local files not available. Skipping Delta preview."
fi

echo -e "\n✅ Demo run complete."
