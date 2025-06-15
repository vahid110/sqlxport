# 🚀 sqlxport Demo: PostgreSQL ➡️ Parquet ➡️ Delta Lake via Spark + MinIO

This demo showcases a full data pipeline using `sqlxport`, converting PostgreSQL data into Parquet, storing it on MinIO, and transforming it into Delta Lake format via Spark.

---

## 🧱 Components

- **PostgreSQL** – Seeded with 3 million `sales` records.
- **MinIO** – S3-compatible storage for staging Parquet files.
- **sqlxport** – Exports data to flat or partitioned Parquet format.
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

Use the unified script to run all scenarios in order:

```bash
./run_sqlxport.sh
```

This script will automatically:

### ① Flat Export (Default)
- Export Parquet to: `sales_delta.parquet`
- Upload to MinIO: `sales_delta/sales_delta.parquet`
- Spark reads and writes Delta to: `delta_output/` (unpartitioned)

### ② Bulk Export: `--partitioned`
- Export partitioned Parquet to: `sales_delta/`
- Spark reads and writes partitioned Delta by `region` to: `sales_delta/`

### ③ Bulk Export: `--output-dir sales_partitioned_delta`
- Export non-partitioned Parquet to: `sales_partitioned_delta.parquet`
- Spark writes flat Delta to: `sales_partitioned_delta/`

### ④ Bulk Export: `--partitioned --output-dir sales_partitioned_delta`
- Export partitioned Parquet to: `sales_partitioned_delta/`
- Spark writes partitioned Delta to: `sales_partitioned_delta/`

Each step runs sequentially, cleaning and verifying outputs.

---

## 🔍 Previewing Results

Use DuckDB to preview partitioned or flat Parquet outputs:

```bash
duckdb -c "SELECT COUNT(*) FROM 'sales_partitioned_delta/**/*.parquet';"
duckdb -c "SELECT * FROM 'delta_output/*.parquet' LIMIT 10;"
```

> Note: DuckDB cannot read Delta metadata. Use it to preview raw `.parquet` files only.

---

## 📂 Files Involved

- `run_sqlxport.sh` – Main demo script. Runs all 4 export scenarios (flat & bulk).
- `run_spark_query.py` – Spark job to convert Parquet ➝ Delta.
- `run_spark_in_docker.sh` – Launches Spark container with query.
- `verify_outputs.sh` – Verifies Delta output content.
- `docker-compose.yml` – Starts PostgreSQL, MinIO, and Spark.
