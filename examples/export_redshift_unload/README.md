# Redshift UNLOAD to S3 (Manual vs sqlxport)

This example shows how to use Redshift UNLOAD manually or via sqlxport.

## Prerequisites

- Amazon Redshift cluster
- S3 bucket with write permissions
- IAM Role attached to Redshift allowing S3 write
- A table named `events`

## Option 1: Manual UNLOAD via psycopg

```bash
python run_manual.py
```
## Option 2: sqlxport CLI

```bash
bash run_sqlxport.sh
```

## Output
Check your S3 bucket for exported Parquet files.