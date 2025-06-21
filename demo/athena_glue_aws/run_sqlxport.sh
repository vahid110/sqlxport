#!/bin/bash
set -e

# Parse CLI args
for arg in "$@"; do
  case $arg in
    --bucket=*)
      BUCKET="${arg#*=}"
      shift
      ;;
    --region=*)
      REGION="${arg#*=}"
      shift
      ;;
    *)
      echo "❌ Unknown argument: $arg"
      echo "Usage: $0 --bucket=your-bucket-name --region=aws-region"
      exit 1
      ;;
  esac
done

if [[ -z "$BUCKET" || -z "$REGION" ]]; then
  echo "❌ Missing --bucket or --region argument"
  echo "Usage: $0 --bucket=your-bucket-name --region=aws-region"
  exit 1
fi

# Auto-detect correct bucket region
DETECTED_REGION=$(aws s3api get-bucket-location --bucket "$BUCKET" --output text)
if [[ "$DETECTED_REGION" == "None" ]]; then
  DETECTED_REGION="us-east-1"
fi
if [[ "$REGION" != "$DETECTED_REGION" ]]; then
  echo "⚠️  WARNING: You passed --region=$REGION, but actual bucket region is $DETECTED_REGION. Using $DETECTED_REGION instead."
  REGION="$DETECTED_REGION"
fi

S3_ENDPOINT="https://s3.${REGION}.amazonaws.com"

DB_URL="postgresql://user:pass@localhost:5432/demo"
GLUE_DB="analytics_demo"
GLUE_TABLE="logs_by_service"
S3_KEY_PREFIX="athena-glue-demo/logs_partitioned/"
S3_OUTPUT="s3://$BUCKET/$S3_KEY_PREFIX"
ATHENA_OUTPUT="s3://$BUCKET/athena-output/"
LOCAL_OUTPUT_DIR="logs_partitioned"

echo "🚀 Starting Athena + Glue export demo using Dockerized PostgreSQL..."

echo "[ 1 / 7 ] Starting PostgreSQL container..."
docker-compose up -d
rm -rf "$LOCAL_OUTPUT_DIR"
aws s3 rm "$S3_OUTPUT" --recursive --quiet --region "$REGION"

echo "[ 2 / 7 ] Exporting logs to S3 in partitioned format..."
sqlxport export \
  --db-url "$DB_URL" \
  --query "SELECT * FROM logs" \
  --output-dir "$LOCAL_OUTPUT_DIR" \
  --format parquet \
  --partition-by service \
  --s3-bucket "$BUCKET" \
  --s3-key "$S3_KEY_PREFIX" \
  --s3-provider aws \
  --export-mode postgres-query \
  --s3-endpoint "$S3_ENDPOINT" \
  --upload-output-dir \
  --aws-region "$REGION"

echo "✅ Export complete. Output: $LOCAL_OUTPUT_DIR"

echo "[ 3 / 7 ] Generating Glue-compatible DDL..."
DDL_INPUT=$(find "$LOCAL_OUTPUT_DIR" -name "*.parquet" | head -n 1)

sqlxport generate-ddl \
  --input-file "$DDL_INPUT" \
  --table-name "$GLUE_TABLE" \
  --partition-by service \
  --s3-url "$S3_OUTPUT" \
  > glue_table.sql


cat glue_table.sql

echo "[ 4 / 7 ] Verifying S3 partition folders exist..."
if ! aws s3 ls "$S3_OUTPUT" --region "$REGION" | grep "service=" > /dev/null; then
  echo "❌ ERROR: No partition folders (service=...) found in S3. Aborting."
  exit 1
fi

echo "[ 5 / 7 ] Waiting 5 seconds to ensure S3 listings are consistent..."
sleep 5

echo "[ 6 / 7 ] Ensuring Glue database '$GLUE_DB' exists..."
aws glue get-database --name "$GLUE_DB" --region "$REGION" >/dev/null 2>&1 || \
aws glue create-database --region "$REGION" --database-input "{\"Name\": \"$GLUE_DB\"}"
echo "[ 7 / 7 ] Registering and repairing Glue table..."
sqlxport postprocess \
  --glue-register \
  --repair-partitions \
  --validate-table \
  --athena-database "$GLUE_DB" \
  --athena-table-name "$GLUE_TABLE" \
  --athena-output "$ATHENA_OUTPUT" \
  --region "$REGION"

echo "✅ Done! You can now query the table in Athena or preview results via:"
echo "   jupyter notebook preview.ipynb"
