# Redshift UNLOAD to S3 (Manual vs sql2data)

This example shows how to use Redshift UNLOAD manually or via sql2data.

## Prerequisites

- Amazon Redshift cluster
- S3 bucket with write permissions
- IAM Role attached to Redshift allowing S3 write
- A table named `events`

## Option 1: Manual UNLOAD via psycopg

```bash
python run_manual.py
```
## Option 2: sql2data CLI

```bash
bash run_sql2data.sh
```

## Output
Check your S3 bucket for exported Parquet files.