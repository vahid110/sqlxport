# Athena + Glue Simulation Demo (Local)

This demo simulates an AWS Athena + Glue Catalog workflow using:

- PostgreSQL as the source database
- MinIO as an S3-compatible object store
- `sqlxport` to export partitioned Parquet
- DuckDB to simulate Athena-style SQL preview

## 🧱 Components

- PostgreSQL for source `logs` table
- `sqlxport` exports logs with partitioning
- MinIO receives files under `athena-demo/logs/`
- DuckDB previews output like Athena would

## 🚀 How to Run

```bash
cd demo/athena_glue_local
./run_sqlxport.sh
```
## 📂 Output
logs_partitioned/ — local copy of partitioned files

MinIO athena-demo/logs/ bucket holds exported files

🦆 Preview (Simulated Athena Query)
```bash
SELECT service, COUNT(*) AS count
FROM 'logs_partitioned/service=*/**/*.parquet'
GROUP BY service;
```