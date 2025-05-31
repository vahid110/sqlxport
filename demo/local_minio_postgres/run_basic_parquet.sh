#!/bin/bash
set -e
echo "🟢 Running basic Parquet export..."

sql2data run \
  --db-url "postgresql://postgres:mysecretpassword@localhost:5432/demo" \
  --query "SELECT * FROM sales" \
  --output-file "sales.parquet" \
  --s3-bucket "demo-bucket" \
  --s3-key "sales.parquet" \
  --s3-endpoint "http://localhost:9000" \
  --s3-access-key "minioadmin" \
  --s3-secret-key "minioadmin"

echo "👀 Previewing local file:"
sql2data run --preview-local-file "sales.parquet"
