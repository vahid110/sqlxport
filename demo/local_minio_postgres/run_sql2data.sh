#!/bin/bash
set -e

# PostgreSQL connection URL
DB_URL="postgresql://postgres:postgres@localhost:5432/demo"

# SQL query
QUERY="SELECT * FROM sales"

# Local output file
OUTPUT_FILE="sales.parquet"

# S3 (MinIO) config
S3_BUCKET="demo-bucket"
S3_KEY="sales.parquet"
S3_ENDPOINT="http://localhost:9000"
S3_ACCESS_KEY="minioadmin"
S3_SECRET_KEY="minioadmin"
AWS_REGION="us-east-1"

# Set credentials for AWS CLI
export AWS_ACCESS_KEY_ID="$S3_ACCESS_KEY"
export AWS_SECRET_ACCESS_KEY="$S3_SECRET_KEY"

# Check and create bucket
echo "🔍 Checking if bucket '$S3_BUCKET' exists..."
if ! aws --endpoint-url "$S3_ENDPOINT" --no-verify-ssl \
        s3api head-bucket --bucket "$S3_BUCKET" 2>/dev/null; then
    echo "🪣 Bucket not found. Creating '$S3_BUCKET'..."
    aws --endpoint-url "$S3_ENDPOINT" --no-verify-ssl \
        s3api create-bucket --bucket "$S3_BUCKET" \
        --region "$AWS_REGION"
    echo "✅ Bucket created."
else
    echo "✅ Bucket '$S3_BUCKET' already exists."
fi

# Run sql2data
sql2data run \
  --db-url "$DB_URL" \
  --query "$QUERY" \
  --output-file "$OUTPUT_FILE" \
  --s3-bucket "$S3_BUCKET" \
  --s3-key "$S3_KEY" \
  --s3-endpoint "$S3_ENDPOINT" \
  --s3-access-key "$S3_ACCESS_KEY" \
  --s3-secret-key "$S3_SECRET_KEY"

# Preview output
echo "👀 Previewing local output file:"
sql2data run --preview-local-file "$OUTPUT_FILE"
