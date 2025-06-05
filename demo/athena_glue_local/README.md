# Athena + Glue Simulation Demo (Local)

This demo simulates an AWS Athena + Glue Catalog workflow using:

- PostgreSQL as the source database
- MinIO as an S3-compatible object store
- `sqlxport` to export partitioned Parquet
- DuckDB to simulate Athena-style SQL preview
- Optional `--glue-register` to emit Glue-compatible DDL

---

## 🧱 Components

- **PostgreSQL**: contains a `logs` table to export
- **sqlxport**: CLI tool used to export `logs` as partitioned Parquet
- **MinIO**: S3-compatible object store for storing results
- **DuckDB**: used locally to preview data like Athena would
- **Glue-compatible DDL**: generated if `--glue-register` is used

---

## 🚀 How to Run

```bash
cd demo/athena_glue_local
./run_sqlxport.sh
```

## 📂 Output
logs_partitioned/ — local copy of partitioned files

s3://athena-demo/logs/ — MinIO bucket path where files are uploaded

glue_table.sql (optional) — sample output of --glue-register

## 🦆 Preview (Simulated Athena Query)
Use DuckDB to simulate querying the exported data:
```bash
SELECT service, COUNT(*) AS count
FROM 'logs_partitioned/service=*/**/*.parquet'
GROUP BY service;
```
Or via CLI:
```bash
duckdb -c "
SELECT service, COUNT(*) AS count
FROM 'logs_partitioned/service=*/**/*.parquet'
GROUP BY service;"
```
## 🧠 Notes

- You can customize output format using --format parquet|csv
- Use --partition-by service or other fields as needed
- Use --glue-register to emit a Glue CREATE EXTERNAL TABLE statement

📚 See Also
run_sqlxport.sh: orchestrates the full pipeline