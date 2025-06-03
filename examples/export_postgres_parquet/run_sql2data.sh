#!/bin/bash
set -e

sql2data run \
  --db-url postgresql://testuser:testpass@localhost:5432/testdb \
  --query "SELECT * FROM users" \
  --output-file users.parquet \
  --format parquet \
  --verbose
