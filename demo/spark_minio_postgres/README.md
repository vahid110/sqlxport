# 🚀 sql2data Bulk Demo: PostgreSQL ➡️ Parquet ➡️ Delta Lake via Spark + MinIO

This demo showcases a full data pipeline using `sql2data`, converting PostgreSQL data into Parquet, storing it on MinIO, and transforming it into Delta Lake format via Spark.

---

## 🧱 Components

- **PostgreSQL** – Seeded with 3 million `sales` records.
- **MinIO** – S3-compatible storage for staging Parquet files.
- **sql2data** – Exports data to partitioned or flat Parquet.
- **Apache Spark + Delta Lake** – Converts Parquet to Delta Lake format.

---

## 🧹 Optional: Reset State

To delete old outputs and demo data:

```bash
docker compose down -v
rm -rf delta_output/ sales_delta/ sales_partitioned_delta/
rm -f sales_delta.parquet sales_partitioned_delta.parquet sales.parquet *.db
docker rm demo-db-spark-minio-postgres demo-minio
docker volume rm demo_minio-data spark_delta_pgdata
```

---

## ▶️ Run the Demo

The demo supports **multiple execution modes**. Here’s what each option does:

---

### ✅ `./run_sql2data.sh`

- Uses flat mode (no partitioning)
- Exports Parquet to: `sales_delta.parquet`
- Uploads to MinIO at: `sales_delta/sales_delta.parquet`
- Spark reads it and writes unpartitioned Delta to: `delta_output/`

---

### ✅ `./run_sql2data_bulk.sh`

#### 🔹 No options

- Uses default values:
  - Partitioned: ❌
  - Output dir: `sales_delta`
- Effectively behaves like `./run_sql2data.sh` (flat mode)

---

### ✅ `./run_sql2data_bulk.sh --partitioned`

- Enables partitioning by `region`
- Output dir defaults to: `sales_delta/`
- Output structure:
  ```
  sales_delta/
    └── data/
        ├── region=EMEA/
        ├── region=NA/
        └── region=APAC/
  ```
- Spark reads partitioned Parquet and writes Delta partitioned by `region`

---

### ✅ `./run_sql2data_bulk.sh --output-dir sales_partitioned_delta`

- Uses flat mode (no `--partitioned` specified)
- Exports single Parquet file to: `sales_partitioned_delta.parquet`
- Spark reads and writes non-partitioned Delta to: `delta_output/`

---

### ✅ `./run_sql2data_bulk.sh --partitioned --output-dir sales_partitioned_delta`

- **Recommended** for bulk mode testing
- Enables region partitioning
- Output dir is explicitly `sales_partitioned_delta/`
- Produces a partitioned Parquet directory and corresponding partitioned Delta table

---

## 🔍 Previewing the Result

Use DuckDB to preview partitioned output recursively:

```bash
duckdb -c "SELECT COUNT(*) FROM 'sales_partitioned_delta/**/*.parquet';"
```

> Note: DuckDB doesn’t support Delta metadata, so we preview raw Parquet files.

---

## 📂 Files Involved

- `run_sql2data_bulk.sh` – Orchestrates both flat and partitioned bulk pipelines.
- `run_sql2data.sh` – Simpler flat-mode demo.
- `run_spark_query.py` – Runs inside Spark container to convert Parquet ➝ Delta.
- `docker-compose.yml` – Sets up PostgreSQL, MinIO, and Spark.
