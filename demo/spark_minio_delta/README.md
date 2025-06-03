# ☁️ sql2data + Spark + Delta Lake + MinIO (S3) Demo

This demo shows how to use [`sql2data`](https://github.com/vahid110/sql2data) to export data from PostgreSQL to Parquet, then transform it using Apache Spark into both **Delta Lake** and **CSV** formats — storing results in **MinIO**, a local S3-compatible object storage system.

---

## 🚀 Overview

- PostgreSQL for source data
- `sql2data` to export table to `sales.parquet`
- Apache Spark converts Parquet to:
  - `s3a://demo/delta_output/` (Delta Lake)
  - `s3a://demo/csv_output/` (CSV)
- All storage and compute runs locally using Docker

---

## 🔧 Requirements

- Docker + Docker Compose
- Python 3 (with `sql2data` CLI installed)
- (Optional) DuckDB or Jupyter for previewing

---

## ▶️ How to Run the Demo

```bash
chmod +x run_sql2data.sh
./run_sql2data.sh
```

This will:
- Start PostgreSQL, MinIO, and Spark services
- Seed the database with sample sales data
- Export to `sales.parquet` via `sql2data`
- Run Spark job to write Delta + CSV to MinIO

---

## 📂 Output (in MinIO)

- `s3a://demo/delta_output/region=.../` → Delta table partitioned by region
- `s3a://demo/csv_output/` → CSV export with header row

---

## 🔍 Optional Preview

To preview files written to MinIO, install `mc` (MinIO Client):

```bash
brew install minio/stable/mc
mc alias set local http://localhost:9000 minioadmin minioadmin
mc ls local/demo/
```

Or preview your `sales.parquet` before Spark:

```bash
duckdb -c "SELECT * FROM 'sales.parquet' LIMIT 5;"
```

---

## 🧼 Cleanup

```bash
docker compose down -v
rm -f sales.parquet
```

---

## 🧠 Notes

- This demo simulates scalable lakehouse patterns using S3-style object storage.
- MinIO can be replaced with AWS S3 for production scenarios.
- Extend this to include schema evolution, versioning, or Redshift-based exports.