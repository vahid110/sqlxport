#!/bin/bash
set -e

# ──────────────────────────────────────────────────────────────
# Step 0: Configuration
# ──────────────────────────────────────────────────────────────
trap 'echo "⚠️  Interrupted. Shutting down..."; docker-compose down; exit 1' INT TERM

unset AWS_PROFILE
unset AWS_SESSION_TOKEN

export LC_ALL=C

DB_URL="postgresql://postgres:mysecretpassword@localhost:5432/demo"
QUERY="SELECT * FROM sales;"

S3_BUCKET="demo-bucket"
S3_ENDPOINT="http://localhost:9000"
S3_ACCESS_KEY="minioadmin"
S3_SECRET_KEY="minioadmin"
AWS_REGION="us-east-1"

export AWS_ACCESS_KEY_ID="$S3_ACCESS_KEY"
export AWS_SECRET_ACCESS_KEY="$S3_SECRET_KEY"

SQLXPORT_BIN="${SQLXPORT_BIN:-sqlxport}"

# ──────────────────────────────────────────────────────────────
# Step 1: Start services
# ──────────────────────────────────────────────────────────────
echo "🧱 Step 1: Starting PostgreSQL and MinIO via docker-compose..."
docker-compose up -d

echo "⏳ Waiting for PostgreSQL to become available..."
until docker exec demo-db-local-minio-postgres pg_isready -U postgres > /dev/null 2>&1; do
  sleep 1
done

# ──────────────────────────────────────────────────────────────
# Step 2: S3 Bucket Check
# ──────────────────────────────────────────────────────────────
echo
echo "🧪 Step 2: Checking/creating bucket '$S3_BUCKET' in MinIO..."
if aws --endpoint-url "$S3_ENDPOINT" s3api head-bucket --bucket "$S3_BUCKET" 2>/dev/null; then
  echo "✅ Bucket '$S3_BUCKET' already exists."
else
  echo "🪣 Bucket not found. Creating '$S3_BUCKET'..."
  aws --endpoint-url "$S3_ENDPOINT" s3api create-bucket --bucket "$S3_BUCKET" \
    || { echo "❌ Failed to create bucket. Check MinIO is running and credentials are correct."; exit 1; }
fi

# ──────────────────────────────────────────────────────────────
# Step 3: Basic Parquet Export
# ──────────────────────────────────────────────────────────────
echo
echo "📦 Step 3: Exporting basic Parquet file..."
echo "Using DB URL: $DB_URL"
echo "Query: $QUERY"
psql "$DB_URL" -c "SELECT * FROM sales;"
OUTPUT_FILE="sales.parquet"
$SQLXPORT_BIN export \
  --db-url "$DB_URL" \
  --query "$QUERY" \
  --output-file "$OUTPUT_FILE" \
  --format parquet \
  --export-mode postgres-query \
  --s3-bucket "$S3_BUCKET" \
  --s3-key "basic-parquet/$OUTPUT_FILE" \
  --s3-endpoint "$S3_ENDPOINT" \
  --s3-access-key "$S3_ACCESS_KEY" \
  --s3-secret-key "$S3_SECRET_KEY"

echo "🔍 Step 4: Previewing basic Parquet file..."
$SQLXPORT_BIN preview --local-file "$OUTPUT_FILE"

echo
echo "🧠 Running AI summary for basic Parquet file..."
$SQLXPORT_BIN preview --local-file "$OUTPUT_FILE" --ai-summary

# ──────────────────────────────────────────────────────────────
# Step 4: CSV Export
# ──────────────────────────────────────────────────────────────
echo
echo "📦 Step 4: Exporting CSV file..."
OUTPUT_FILE="sales.csv"
$SQLXPORT_BIN export \
  --db-url "$DB_URL" \
  --query "$QUERY" \
  --output-file "$OUTPUT_FILE" \
  --format csv \
  --export-mode postgres-query \
  --s3-bucket "$S3_BUCKET" \
  --s3-key "csv/$OUTPUT_FILE" \
  --s3-endpoint "$S3_ENDPOINT" \
  --s3-access-key "$S3_ACCESS_KEY" \
  --s3-secret-key "$S3_SECRET_KEY"

echo "🔍 Previewing CSV file (head -n 10):"
head -n 10 "$OUTPUT_FILE"

# ──────────────────────────────────────────────────────────────
# Step 5: Partitioned Parquet Export
# ──────────────────────────────────────────────────────────────
echo
echo "📦 Step 5: Exporting partitioned Parquet dataset..."
OUTPUT_DIR="output_partitioned"
$SQLXPORT_BIN export \
  --db-url "$DB_URL" \
  --query "$QUERY" \
  --output-dir "$OUTPUT_DIR" \
  --partition-by "region" \
  --format parquet \
  --export-mode postgres-query \
  --s3-bucket "$S3_BUCKET" \
  --s3-key "partitioned-parquet/" \
  --s3-endpoint "$S3_ENDPOINT" \
  --s3-access-key "$S3_ACCESS_KEY" \
  --s3-secret-key "$S3_SECRET_KEY" \
  --upload-output-dir

echo "🔍 Previewing partitions using DuckDB:"
duckdb -c "SELECT region, COUNT(*) FROM read_parquet('output_partitioned/*/*.parquet') GROUP BY region;"

# ──────────────────────────────────────────────────────────────
# Step 6: Exporting Complex Join Query
# ──────────────────────────────────────────────────────────────
echo
echo "🔁 Step 6: Exporting complex join query from PostgreSQL..."
OUTPUT_DIR="output_complex_postgres"
rm -rf "$OUTPUT_DIR"
$SQLXPORT_BIN export \
  --db-url "$DB_URL" \
  --query "
    WITH customer_totals AS (
      SELECT
        c.name,
        c.country,
        COUNT(o.id) AS order_count,
        SUM(o.total_amount) AS total_spent
      FROM customers c
      JOIN orders o ON c.id = o.customer_id
      WHERE c.country = 'Germany'
      GROUP BY c.name, c.country
    )
    SELECT * FROM customer_totals
    ORDER BY total_spent DESC
  " \
  --output-dir "$OUTPUT_DIR" \
  --format parquet \
  --export-mode postgres-query \

echo
echo "🔍 Previewing output_complex_postgres with DuckDB:"
duckdb -c "
  SELECT country, COUNT(*) AS customer_groups, SUM(total_spent) AS total_spent
  FROM read_parquet('output_complex_postgres/*.parquet')
  GROUP BY country;
"

echo
echo "🧪 sqlxport preview of output_complex_postgres:"
$SQLXPORT_BIN preview --local-dir output_complex_postgres

# ──────────────────────────────────────────────────────────────
# Step 7: Cleanup
# ──────────────────────────────────────────────────────────────
echo
echo "🧼 Step 7: Stopping Docker services..."
docker-compose down

echo "🧹 Cleaning up local output files..."
rm -f sales.parquet sales.csv
rm -rf output_partitioned
rm -rf output_complex_postgres

echo
echo "✅ All exports and previews completed successfully."
