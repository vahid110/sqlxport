# sql2data Local Demo (Self-Run, Free – Hybrid Edition)

This demo lets you extract data from PostgreSQL, export to Parquet (basic or partitioned) or CSV using `sql2data`, upload to MinIO (S3-compatible), and query or preview it locally using CLI, DuckDB, or Jupyter.

---

## 🚀 Quick Start

### 1. Requirements

- Docker & Docker Compose
- `sql2data` CLI installed and in your `$PATH`
- AWS CLI (`brew install awscli`)
- (Optional) DuckDB or Jupyter for previewing output

---

### 2. Start Services

```bash
docker-compose up -d
```

Wait 10 seconds for PostgreSQL and MinIO to start.

---

## 🔧 Export Options

### ✅ 1. Basic Parquet Export

```bash
./run_basic_parquet.sh
```

- Saves to `sales.parquet`
- Uploads to `demo-bucket/sales.parquet`
- Shows preview using `--preview-local-file`

---

### ✅ 2. Partitioned Parquet Export

```bash
./run_partitioned_parquet.sh
```

- Saves partitioned Parquet files to `output_partitioned/region=...`
- Uploads all to MinIO

---

### ✅ 3. CSV Export

```bash
./run_csv.sh
```

- Saves a flat `sales.csv`
- Previews it using `head`

---

## 🔍 Preview Output (Optional)

### ✅ Use DuckDB to query the Parquet file:

#### ✅ Basic Parquet

```bash
duckdb
SELECT * FROM 'sales.parquet' LIMIT 10;
```

Or run predefined query:
```bash
duckdb -c "SELECT region, SUM(amount) FROM 'sales.parquet' GROUP BY region"
```

#### ✅ Partitioned Parquet
DuckDB can read partitioned files using read_parquet():
```bash
duckdb -c "SELECT * FROM read_parquet('output_partitioned/*/*.parquet') LIMIT 10"
```
Or to aggregate:

```bash
duckdb -c "SELECT region, SUM(amount) FROM read_parquet('output_partitioned/*/*.parquet') GROUP BY region"
```

#### ✅ CSV File
```bash
duckdb
SELECT * FROM 'sales.csv' LIMIT 10;
```
Or run:

```bash
duckdb -c "SELECT * FROM 'sales.csv'"
```
---

### ✅ Use Jupyter Notebook (Recommended for Analysts)

```bash
jupyter notebook preview.ipynb
```

---

### ✅ Use MinIO Console

Access MinIO web UI at:  
[http://localhost:9001](http://localhost:9001)  
Login: `minioadmin` / `minioadmin`

---

## ✅ Cleanup

```bash
docker-compose down -v
```

---

## 🧠 Notes

- PostgreSQL comes pre-seeded with a `sales` table.
- All services run locally, no cloud required.
- You can swap in Redshift or Athena later with minimal changes.

Enjoy!
