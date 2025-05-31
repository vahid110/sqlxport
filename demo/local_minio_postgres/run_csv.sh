#!/bin/bash
set -e
echo "🔵 Running CSV export..."

sql2data run \
  --db-url "postgresql://postgres:mysecretpassword@localhost:5432/demo" \
  --query "SELECT * FROM sales" \
  --format "csv" \
  --output-file "sales.csv"

echo "👀 Previewing CSV:"
head -n 10 sales.csv
