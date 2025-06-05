# Athena + Glue Export Demo with sqlxport

This demo shows how to export data from PostgreSQL to Amazon S3 in Parquet format using sqlxport, register it in the AWS Glue Data Catalog, and query it via Amazon Athena.

## 🚀 Quick Start
```bash
./run_sql2data.sh --bucket=<your-s3-bucket> --region=<aws-region>
```

### Required Arguments

--bucket: Your S3 bucket name (e.g., vahid-signing)

--region: AWS region (e.g., us-east-1)

💡 This demo script avoids hardcoding your bucket and region.

## 🔧 What It Does

Step-by-step:

- Starts Dockerized PostgreSQL with sample data.

- Exports query results to local folder as partitioned Parquet files.

- Uploads partitioned files to S3.

- Generates Athena-compatible CREATE EXTERNAL TABLE DDL.

- Registers the table in the Glue Catalog.

- Repairs partitions using MSCK REPAIR TABLE.

- Validates table registration with a sample Athena query.

## 🗂 Project Structure
```
demo/athena_glue_aws/
├── run_sql2data.sh       # End-to-end script (parametrized)
├── docker-compose.yml    # PostgreSQL container
├── glue_table.sql        # Generated DDL (auto-overwritten)
└── logs_partitioned/     # Local partitioned output (auto-removed)
```
## 🧪 Sample Athena Query Used for Validation

SELECT service, COUNT(*) AS count FROM logs_by_service GROUP BY service;

## 📌 Notes

The script checks that partition folders (e.g., service=auth/) exist in S3 before continuing.

A short delay is added after S3 upload to ensure consistency.

You can safely re-run the script; it cleans up local and remote state each time.

## ✅ Requirements

- AWS CLI (configured)

- Docker

- Python + sqlxport installed (or installed via pip install -e .[dev])

## 📤 Outputs

- Partitioned Parquet files in S3

- Glue table: analytics_demo.logs_by_service

- Athena query results under: s3://<your-bucket>/athena-output/

## 🧭 Next Steps (To-Do)

Auto-bootstrap missing partitions if MSCK fails

Optional: Push --glue-register, --repair-partitions, and --validate-table logic into sqlxport

## 🙌 Credits

Made with ❤️ using sqlxport and AWS Glue/Athena

