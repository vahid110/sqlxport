# Redshift UNLOAD → Glue → Athena Demo

This demo shows how to export data from **Amazon Redshift** to **Amazon S3** using the `UNLOAD` command, generate a Glue-compatible table definition with `sqlxport`, register it in the **AWS Glue Catalog**, and validate with **Amazon Athena**.

## 🚀 Quick Start

```bash
./run_sqlxport.sh <s3-bucket> <aws-region>
```

## 📦 What It Does

1. Bootstraps a Redshift table (`logs`) with sample data.
2. Runs `UNLOAD` to export the table to S3 in Parquet format.
3. Downloads a sample `.parquet` file locally.
4. Generates Athena-compatible `CREATE EXTERNAL TABLE` DDL.
5. Registers the Glue table via Athena.
6. Repairs partitions (if needed).
7. Validates visibility with an Athena `COUNT(*)` query.

## 📂 Folder Contents

```
redshift_to_athena/
├── run_sqlxport.sh        # Full automation script
├── glue_table.sql         # Generated DDL (auto-overwritten)
├── tmp_unload/            # Temporary folder with downloaded .parquet
└── preview_redshift_unload.ipynb  # Optional Jupyter preview
```

## 🧪 Requirements

- A Redshift cluster with UNLOAD permissions.
- A valid IAM role for S3 access.
- AWS CLI configured
- Python + `sqlxport` installed
- Jupyter (optional for preview)

## 📝 Notes

- IAM role and Redshift DB URL must be provided via environment variables:
  - `REDSHIFT_DB_URL`
  - `REDSHIFT_IAM_ROLE`
- The script cleans previous outputs each time you run it.
- It works with the real Redshift, not a mock or containerized DB.

## 📊 Sample Athena Query Used for Validation

```sql
SELECT COUNT(*) FROM logs_unload;
```

## 🙌 Credits

Made with ❤️ using `sqlxport`, AWS Redshift, Glue, and Athena.
