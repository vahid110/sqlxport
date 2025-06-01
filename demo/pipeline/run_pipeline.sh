#!/bin/bash
set -e

# ------------------------------
# Pipeline Phase 1: Extract + Store to S3
# ------------------------------

# Defaults
PARTITIONED=0
FORMAT="parquet"
OUTPUT_DIR="pipeline_output"
USE_S3=0
MC_CONFIG_DIR=".mc-config"

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

# Config
DB_URL="postgresql://postgres:postgres@localhost:5432/demo"
QUERY="SELECT * FROM sales"
S3_BUCKET="demo-bucket"
S3_ENDPOINT="http://localhost:9000"
S3_ACCESS_KEY="minioadmin"
S3_SECRET_KEY="minioadmin"

# Step 1: Export
mkdir -p "$OUTPUT_DIR"
echo "📤 Exporting from DB to local $FORMAT..."
if [[ "$PARTITIONED" -eq 1 ]]; then
  echo "💾 Saving partitioned output to $OUTPUT_DIR..."
  sql2data run \
    --db-url "$DB_URL" \
    --query "$QUERY" \
    --output-dir "$OUTPUT_DIR" \
    --format "$FORMAT" \
    --partition-by region
else
  echo "💾 Saving flat output to $OUTPUT_DIR..."
  sql2data run \
    --db-url "$DB_URL" \
    --query "$QUERY" \
    --output-dir "$OUTPUT_DIR" \
    --format "$FORMAT"
fi
echo "✅ Done."

# Step 2: Upload to S3
if [[ "$USE_S3" -eq 1 ]]; then
  echo "🪣 Registering MinIO alias 'local' for bucket '$S3_BUCKET'..."
  mkdir -p "$MC_CONFIG_DIR"

  docker run --rm \
    -v "$(pwd)/$MC_CONFIG_DIR":/root/.mc \
    --network host \
    minio/mc alias set local "$S3_ENDPOINT" "$S3_ACCESS_KEY" "$S3_SECRET_KEY"

  echo "📦 Creating bucket if it doesn't exist..."
  if ! docker run --rm \
    -v "$(pwd)/$MC_CONFIG_DIR":/root/.mc \
    --network host \
    minio/mc ls local/"$S3_BUCKET" >/dev/null 2>&1; then
    docker run --rm \
      -v "$(pwd)/$MC_CONFIG_DIR":/root/.mc \
      --network host \
      minio/mc mb -p local/"$S3_BUCKET"
  fi

  echo "☁️ Uploading '$OUTPUT_DIR' to local/$S3_BUCKET ..."
  docker run --rm \
    -v "$(pwd)/$MC_CONFIG_DIR":/root/.mc \
    -v "$(pwd)/$OUTPUT_DIR":/data \
    --network host \
    minio/mc cp --recursive /data local/"$S3_BUCKET"/"$OUTPUT_DIR"

  echo "✅ Upload complete."

  echo "📄 Listing uploaded files in bucket..."
  docker run --rm \
    -v "$(pwd)/$MC_CONFIG_DIR":/root/.mc \
    --network host \
    minio/mc ls --recursive local/"$S3_BUCKET"/"$OUTPUT_DIR"
else
  echo "🗃️ Output written locally to '$OUTPUT_DIR/'. No S3 upload."
  echo "✅ Phase 1 pipeline complete."
fi
