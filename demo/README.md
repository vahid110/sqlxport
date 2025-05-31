# sql2data Local Demo (Self-Run, Free)

This demo lets you extract data from PostgreSQL, export to Parquet using `sql2data`, upload to MinIO (S3-compatible), and query it locally.

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

### 3. Export Data

```bash
chmod +x run_sql2data.sh
./run_sql2data.sh
```

This runs `sql2data` to extract the `sales` table to Parquet, uploads it to MinIO, and previews the result.

---

### 4. Preview Output (Optional)

#### ✅ Use DuckDB to query the Parquet file:

```bash
duckdb
SELECT * FROM 'sales.parquet' LIMIT 10;
```

Or run predefined query:
```bash
duckdb -c "$(cat preview_query.duckdb)"
```

#### ✅ Use Jupyter Notebook (Recommended for Analysts)

```bash
jupyter notebook preview.ipynb
```

---

### 5. MinIO Console

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
