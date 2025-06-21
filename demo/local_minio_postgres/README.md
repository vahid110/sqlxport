# 🧪 sqlxport Local Demo (Self-Run, Hybrid Edition)

This demo showcases a local data pipeline using `sqlxport`, PostgreSQL, and MinIO to export data into Parquet or CSV, preview it locally using DuckDB or CLI, and clean up afterward. All services run locally using Docker.

---

## 🚀 Quick Start

### 1. Prerequisites

- Docker & Docker Compose
- `sqlxport` installed and in `$PATH`
- AWS CLI (`brew install awscli`)
- Optional: DuckDB and Jupyter Notebook

---

### 2. Run the Full Pipeline

Use the consolidated script:

```bash
./run_sqlxport.sh
```

This will:

1. 🧱 Start PostgreSQL and MinIO via Docker.
2. 🪣 Create `demo-bucket` in MinIO if missing.
3. 📦 Export `sales` table to:
   - Basic Parquet: `sales.parquet` → `basic-parquet/sales.parquet`
   - CSV: `sales.csv` → `csv/sales.csv`
   - Partitioned Parquet: `output_partitioned/region=...` → `partitioned-parquet/`
4. 🔍 Preview local results with DuckDB or CLI.
5. 🧼 Tear down containers and clean local outputs.

---

## 🔍 Optional Preview Methods

### ✅ DuckDB

- Basic:
  ```bash
  duckdb -c "SELECT region, SUM(amount) FROM 'sales.parquet' GROUP BY region;"
  ```

- Partitioned:
  ```bash
  duckdb -c "SELECT region, COUNT(*) FROM read_parquet('output_partitioned/*/*.parquet') GROUP BY region;"
  ```

- CSV:
  ```bash
  duckdb -c "SELECT * FROM 'sales.csv' LIMIT 10;"
  ```

### ✅ Jupyter

```bash
jupyter notebook preview.ipynb
```

---

### ✅ MinIO Console

Access: [http://localhost:9001](http://localhost:9001)  
Credentials: `minioadmin / minioadmin`

---

## 🧹 Cleanup

The script automatically stops containers and removes:

- Local files: `sales.parquet`, `sales.csv`, `output_partitioned/`
- Containers: PostgreSQL, MinIO

You can also run:

```bash
docker-compose down -v
```

---

## 🧠 Notes

- PostgreSQL is pre-seeded with a `sales` table.
- `sqlxport` uploads all results to MinIO using S3-compatible API.
- No external cloud access is required.
