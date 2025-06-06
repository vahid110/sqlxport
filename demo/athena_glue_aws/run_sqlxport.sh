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

DB_URL="postgresql://user:pass@localhost:5432/demo"
GLUE_DB="analytics_demo"
GLUE_TABLE="logs_by_service"
S3_KEY_PREFIX="athena-glue-demo/logs_partitioned/"
S3_OUTPUT="s3://$BUCKET/$S3_KEY_PREFIX"
ATHENA_OUTPUT="s3://$BUCKET/athena-output/"
LOCAL_OUTPUT_DIR="logs_partitioned"

echo "🚀 Starting Athena + Glue export demo using Dockerized PostgreSQL..."

echo "[ 1 / 8 ] Starting PostgreSQL container..."
docker-compose up -d
rm -rf "$LOCAL_OUTPUT_DIR"
aws s3 rm "$S3_OUTPUT" --recursive --quiet --region "$REGION"

echo "[ 2 / 8 ] Exporting logs to S3 in partitioned format..."
sqlxport run \
  --db-url "$DB_URL" \
  --query "SELECT * FROM logs" \
  --output-dir "$LOCAL_OUTPUT_DIR" \
  --format parquet \
  --partition-by service \
  --s3-bucket "$BUCKET" \
  --s3-key "$S3_KEY_PREFIX" \
  --s3-provider aws \
  --s3-endpoint "https://s3.amazonaws.com" \
  --upload-output-dir

echo "[ 3 / 8 ] Generating Glue-compatible DDL..."
sqlxport run \
  --generate-athena-ddl "$LOCAL_OUTPUT_DIR" \
  --output-dir "$LOCAL_OUTPUT_DIR" \
  --athena-s3-prefix "$S3_OUTPUT" \
  --athena-table-name "$GLUE_TABLE" \
  --partition-by service > glue_table.sql

cat glue_table.sql

echo "[ 4 / 8 ] Verifying S3 partition folders exist..."
if ! aws s3 ls "$S3_OUTPUT" --region "$REGION" | grep "service=" > /dev/null; then
  echo "❌ ERROR: No partition folders (service=...) found in S3. Aborting."
  exit 1
fi

echo "[ 5 / 8 ] Waiting 5 seconds to ensure S3 listings are consistent..."
sleep 5

echo "[ 6 / 8 ] Registering table in Athena Glue Catalog..."
REGISTER_OUTPUT=$(aws athena start-query-execution \
  --region "$REGION" \
  --query-string file://glue_table.sql \
  --query-execution-context Database="$GLUE_DB" \
  --result-configuration OutputLocation="$ATHENA_OUTPUT")

REGISTER_ID=$(echo "$REGISTER_OUTPUT" | jq -r .QueryExecutionId)
echo "✅ Glue table '$GLUE_TABLE' registered."

echo "[ 7 / 8 ] Repairing partitions in Glue table..."
REPAIR_OUTPUT=$(aws athena start-query-execution \
  --region "$REGION" \
  --query-string "MSCK REPAIR TABLE $GLUE_TABLE;" \
  --query-execution-context Database="$GLUE_DB" \
  --result-configuration OutputLocation="$ATHENA_OUTPUT")

REPAIR_ID=$(echo "$REPAIR_OUTPUT" | jq -r .QueryExecutionId)
echo "   → MSCK REPAIR QueryExecutionId: $REPAIR_ID"

# Wait for MSCK to complete
for i in {1..20}; do
  STATE=$(aws athena get-query-execution \
    --region "$REGION" \
    --query-execution-id "$REPAIR_ID" \
    --query 'QueryExecution.Status.State' \
    --output text)

  echo "   → Repair state: $STATE"
  if [[ "$STATE" == "SUCCEEDED" ]]; then
    echo "✅ Partition repair completed."
    break
  elif [[ "$STATE" == "FAILED" || "$STATE" == "CANCELLED" ]]; then
    echo "❌ MSCK REPAIR failed."
    aws athena get-query-execution \
      --region "$REGION" \
      --query-execution-id "$REPAIR_ID" \
      --query 'QueryExecution.Status.StateChangeReason' \
      --output text
    exit 1
  fi
  sleep 2
done

echo "[ 8 / 8 ] Validating Glue table with Athena query..."
QUERY="SELECT service, COUNT(*) AS count FROM $GLUE_TABLE GROUP BY service;"

EXECUTION_ID=$(aws athena start-query-execution \
  --region "$REGION" \
  --query-string "$QUERY" \
  --query-execution-context Database="$GLUE_DB" \
  --result-configuration OutputLocation="$ATHENA_OUTPUT" \
  --query 'QueryExecutionId' \
  --output text)

echo "🔎 Athena query execution ID: $EXECUTION_ID"
echo "⏳ Waiting for query result..."

for i in {1..20}; do
  STATE=$(aws athena get-query-execution \
    --region "$REGION" \
    --query-execution-id "$EXECUTION_ID" \
    --query 'QueryExecution.Status.State' \
    --output text)

  echo "   → Current state: $STATE"
  if [[ "$STATE" == "SUCCEEDED" ]]; then
    echo "✅ Athena query ran successfully."
    exit 0
  elif [[ "$STATE" == "FAILED" || "$STATE" == "CANCELLED" ]]; then
    echo "❌ Athena query failed with state: $STATE"
    echo "   🔍 Fetching failure reason..."
    aws athena get-query-execution \
      --region "$REGION" \
      --query-execution-id "$EXECUTION_ID" \
      --query 'QueryExecution.Status.StateChangeReason' \
      --output text
    exit 1
  fi
  sleep 2
done

echo "❌ Timed out waiting for Athena query."
exit 1
