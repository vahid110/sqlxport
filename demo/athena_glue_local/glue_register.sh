#!/bin/bash
# demo/athena_glue_local/glue_register.sh
# Simulate Glue catalog registration via Athena-compatible DDL

set -e

OUTPUT_DIR="logs_partitioned"
DDL_FILE="ddl/logs_table.sql"
TABLE_NAME="logs_table"
S3_PREFIX="logs/"

mkdir -p ddl

echo -e "\n📜 Generating Athena DDL for table: $TABLE_NAME..."
sqlxport export \
  --generate-athena-ddl "$OUTPUT_DIR" \
  --athena-s3-prefix "$S3_PREFIX" \
  --athena-table-name "$TABLE_NAME" \
  > "$DDL_FILE"

echo -e "\n✅ Athena DDL written to: $DDL_FILE"
echo -e "\n📄 Contents:\n"
cat "$DDL_FILE"

echo -e "\n📘 Next steps:\n"
echo " 1. Run this DDL manually in AWS Athena console."
echo " 2. Or use AWS Glue API/CLI to register this schema (Pro feature)."
