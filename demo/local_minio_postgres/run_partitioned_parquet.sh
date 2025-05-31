#!/bin/bash
set -e
echo "🟡 Running partitioned Parquet export..."

sql2data run \
  --db-url "postgresql://postgres:mysecretpassword@localhost:5432/demo" \
  --query "SELECT * FROM sales" \
  --output-dir "output_partitioned" \
  --partition-by "region" \
  --format "parquet" \
  --s3-bucket "demo-bucket" \
  --s3-key "sales_partitioned" \
  --s3-endpoint "http://localhost:9000" \
  --s3-access-key "minioadmin" \
  --s3-secret-key "minioadmin" \
  --upload-output-dir
