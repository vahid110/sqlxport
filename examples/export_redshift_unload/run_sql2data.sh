#!/bin/bash
set -e

sql2data run \
  --use-redshift-unload \
  --db-url redshift+psycopg2://awsuser:pass@endpoint:5439/dev \
  --query "SELECT * FROM events" \
  --s3-output-prefix s3://my-bucket/unload/events_ \
  --iam-role arn:aws:iam::123456789012:role/MyUnloadRole \
  --verbose
