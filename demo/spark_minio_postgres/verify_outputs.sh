#!/bin/bash
set -e

OUTPUT_DIR="$1"

if [[ -z "$OUTPUT_DIR" ]]; then
  echo "Usage: $0 <delta_output_folder>"
  exit 1
fi

echo "🔍 Previewing Delta folder: $OUTPUT_DIR"

# Detect whether output is partitioned
PARQUET_FILES=$(find "$OUTPUT_DIR" -type f -name "*.parquet")

if [[ -z "$PARQUET_FILES" ]]; then
  echo "🔴 No Parquet files found in $OUTPUT_DIR"
  exit 1
fi

# Show partition-wise row counts if partitioned
if compgen -G "$OUTPUT_DIR/region=*/" > /dev/null; then
  echo -e "\n📊 Row count by partition (partitioned by region):"
  duckdb -c "SELECT region, COUNT(*) AS count FROM '${OUTPUT_DIR}/region=*/**.parquet' GROUP BY region ORDER BY count DESC;"
else
  echo -e "\n📊 Row count:"
  duckdb -c "SELECT COUNT(*) AS count FROM '${OUTPUT_DIR}/*.parquet';"
fi

# List files
echo -e "\n📁 File listing:"
find "$OUTPUT_DIR" -type f -name "*.parquet" -exec ls -lh {} \;

echo -e "\n✅ Validation complete."
