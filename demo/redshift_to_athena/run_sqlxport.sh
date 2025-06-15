#!/bin/bash
set -e

# Accept arguments or default fallback
: "${REDSHIFT_DB_URL:="postgresql://USER:PWD@CLUSTER.redshift.amazonaws.com:5439/dev"}"
: "${REDSHIFT_IAM_ROLE:="arn:aws:iam::12345678:role/your_redshift_and_s3_access_role"}"
: "${S3_BUCKET:=$1}"
: "${S3_REGION:=$2}"
: "${GLUE_DB:=analytics_demo}"
: "${GLUE_TABLE:=logs_unload}"
: "${S3_OUTPUT_PREFIX:=redshift-unload-demo/logs/}"
: "${ATHENA_OUTPUT:=s3://$S3_BUCKET/athena-output/}"

if [[ -z "$S3_BUCKET" || -z "$S3_REGION" ]]; then
  echo "❌ Usage: $0 <s3-bucket-name> <aws-region>"
  exit 1
fi

# [ 1 / 8 ] Bootstrap Redshift table
echo "[ 1 / 8 ] Bootstrapping Redshift table..."

# Strip protocol prefix
CLEAN_URL="${REDSHIFT_DB_URL#*://}"

# Separate credentials and host/db
CREDENTIALS="${CLEAN_URL%@*}"
HOST_AND_DB="${CLEAN_URL#*@}"

REDSHIFT_USER="${CREDENTIALS%%:*}"
REDSHIFT_PASSWORD="${CREDENTIALS#*:}"

REDSHIFT_HOST_PORT="${HOST_AND_DB%%/*}"
REDSHIFT_DB="${HOST_AND_DB#*/}"

REDSHIFT_HOST="${REDSHIFT_HOST_PORT%%:*}"
REDSHIFT_PORT="${REDSHIFT_HOST_PORT##*:}"

REDSHIFT_REGION=$(echo "$REDSHIFT_HOST" | sed -E 's|.*\.([a-z0-9-]+)\.redshift\.amazonaws\.com|\1|')
REDSHIFT_CLUSTER_ID=$(echo "$REDSHIFT_HOST" | cut -d'.' -f1)

SQL="
DROP TABLE IF EXISTS logs;
CREATE TABLE logs (
  id INT,
  service VARCHAR(50),
  message VARCHAR(255),
  timestamp TIMESTAMP
);
INSERT INTO logs VALUES
  (1, 'auth', 'User login', GETDATE()),
  (2, 'billing', 'Payment processed', GETDATE()),
  (3, 'auth', 'Password reset', GETDATE());
"

QUERY_ID=$(aws redshift-data execute-statement \
  --region "$REDSHIFT_REGION" \
  --cluster-identifier "$REDSHIFT_CLUSTER_ID" \
  --database "$REDSHIFT_DB" \
  --db-user "$REDSHIFT_USER" \
  --sql "$SQL" \
  --output text \
  --query 'Id')

while true; do
  STATUS=$(aws redshift-data describe-statement \
    --region "$REDSHIFT_REGION" \
    --id "$QUERY_ID" \
    --query 'Status' \
    --output text)
  echo "   → Redshift bootstrap status: $STATUS"
  [[ "$STATUS" == "FINISHED" ]] && break
  [[ "$STATUS" == "FAILED" || "$STATUS" == "ABORTED" ]] && {
    echo "❌ Redshift seeding failed."
    exit 1
  }
  sleep 2
done

S3_OUTPUT="s3://$S3_BUCKET/$S3_OUTPUT_PREFIX"

# [ 2 / 8 ] Clean previous output
echo "[ 2 / 8 ] Cleaning S3 output paths..."
aws s3 rm "$S3_OUTPUT" --recursive --quiet || true
aws s3 rm "$ATHENA_OUTPUT" --recursive --quiet || true

# [ 3 / 8 ] Export using Redshift UNLOAD
echo "[ 3 / 8 ] Exporting with Redshift UNLOAD to $S3_OUTPUT..."
sqlxport export \
  --db-url "$REDSHIFT_DB_URL" \
  --iam-role "$REDSHIFT_IAM_ROLE" \
  --query "SELECT * FROM logs" \
  --use-redshift-unload \
  --s3-bucket "$S3_BUCKET" \
  --s3-key "$S3_OUTPUT_PREFIX" \
  --s3-provider aws \
  --s3-endpoint "https://s3.$S3_REGION.amazonaws.com"

# [ 4 / 8 ] Download one sample file locally for DDL
echo "[ 4 / 8 ] Downloading sample .parquet for local inspection..."
mkdir -p tmp_unload && rm -f tmp_unload/*.parquet
aws s3 cp --recursive "$S3_OUTPUT" tmp_unload/ \
  --exclude "*" --include "*.parquet"

# [ 5 / 8 ] Generate Glue-compatible DDL
echo "[ 5 / 8 ] Generating Glue-compatible DDL..."
sqlxport export \
  --generate-athena-ddl "tmp_unload" \
  --athena-s3-prefix "$S3_OUTPUT" \
  --athena-table-name "$GLUE_TABLE" \
  > glue_table.sql

# [ 6 / 8 ] Register table in Glue Catalog via Athena
echo "[ 6 / 8 ] Registering table in Glue Catalog..."
REGISTER_ID=$(aws athena start-query-execution \
  --region "$S3_REGION" \
  --query-string file://glue_table.sql \
  --query-execution-context Database="$GLUE_DB" \
  --result-configuration OutputLocation="$ATHENA_OUTPUT" \
  --output text \
  --query 'QueryExecutionId')

for i in {1..10}; do
  STATUS=$(aws athena get-query-execution \
    --region "$S3_REGION" \
    --query-execution-id "$REGISTER_ID" \
    --query 'QueryExecution.Status.State' \
    --output text)
  [[ "$STATUS" == "SUCCEEDED" ]] && break
  [[ "$STATUS" == "FAILED" || "$STATUS" == "CANCELLED" ]] && echo "❌ Table registration failed." && exit 1
  sleep 2
done

# [ 7 / 8 ] Repair partitions (if any)
echo "[ 7 / 8 ] Repairing partitions (if applicable)..."
REPAIR_ID=$(aws athena start-query-execution \
  --region "$S3_REGION" \
  --query-string "MSCK REPAIR TABLE $GLUE_TABLE;" \
  --query-execution-context Database="$GLUE_DB" \
  --result-configuration OutputLocation="$ATHENA_OUTPUT" \
  --output text \
  --query 'QueryExecutionId')

for i in {1..10}; do
  STATUS=$(aws athena get-query-execution \
    --region "$S3_REGION" \
    --query-execution-id "$REPAIR_ID" \
    --query 'QueryExecution.Status.State' \
    --output text)
  [[ "$STATUS" == "SUCCEEDED" ]] && break
  [[ "$STATUS" == "FAILED" || "$STATUS" == "CANCELLED" ]] && echo "❌ MSCK failed." && exit 1
  sleep 2
done

# [ 8 / 8 ] Validate final row count in Athena
echo "[ 8 / 8 ] Running Athena validation query..."
QUERY="SELECT COUNT(*) AS total FROM $GLUE_TABLE;"
VALIDATE_ID=$(aws athena start-query-execution \
  --region "$S3_REGION" \
  --query-string "$QUERY" \
  --query-execution-context Database="$GLUE_DB" \
  --result-configuration OutputLocation="$ATHENA_OUTPUT" \
  --output text \
  --query 'QueryExecutionId')

for i in {1..10}; do
  STATUS=$(aws athena get-query-execution \
    --region "$S3_REGION" \
    --query-execution-id "$VALIDATE_ID" \
    --query 'QueryExecution.Status.State' \
    --output text)
  [[ "$STATUS" == "SUCCEEDED" ]] && break
  [[ "$STATUS" == "FAILED" || "$STATUS" == "CANCELLED" ]] && echo "❌ Validation query failed." && exit 1
  sleep 2
done

echo "✅ Redshift UNLOAD → Glue → Athena demo completed successfully."

# Show result
RESULT_FILE="athena_query_results.json"
aws athena get-query-results \
  --region "$S3_REGION" \
  --query-execution-id "$VALIDATE_ID" \
  --output json > "$RESULT_FILE"

ROW_COUNT=$(jq -r '.ResultSet.Rows[1].Data[0].VarCharValue' "$RESULT_FILE")
echo "🔍 Total row count in Glue table: $ROW_COUNT"

cat glue_table.sql

echo "\nRun 'jupyter notebook preview.ipynb' for a preview"
