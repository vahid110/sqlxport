# 🛠️ sql2data Pipeline Demo (Phase 1)

This demo shows a realistic but minimal ETL pipeline built with **bash scripts** using `sql2data`, DuckDB, and optionally MinIO/S3 as the target.

---

## 🎯 Goal

Simulate a production-like ETL pipeline that:
- Extracts data from PostgreSQL
- Transforms and optionally partitions it (Parquet/CSV)
- Validates and previews output
- Stores to local or S3-compatible bucket

---

## 📦 Requirements

- Docker + Docker Compose
- Python 3 (`sql2data`, `pandas`, `duckdb`)
- Optional: MinIO or real S3 credentials

---

## 🚦 How to Run

```bash
./run_pipeline.sh [--partitioned] [--use-s3] [--format csv|parquet] [--output-dir my_output]
```

### Examples

- Local, flat Parquet:
  ```bash
  ./run_pipeline.sh
  ```

- Local, partitioned Parquet:
  ```bash
  ./run_pipeline.sh --partitioned
  ```

- Upload to S3/MinIO:
  ```bash
  ./run_pipeline.sh --use-s3 --partitioned --format parquet --output-dir sales_data
  ```

---

## 🔄 Stages in the Pipeline

| Stage     | Description                            | Tool         |
|-----------|----------------------------------------|--------------|
| Extract   | Run SQL query and fetch results        | sql2data     |
| Transform | Save to Parquet/CSV                    | sql2data     |
| Validate  | Check row count and preview sample     | DuckDB/Pandas|
| Load      | Store locally or to S3/MinIO           | sql2data/mc  |

---

## 🧪 Output

Output files are saved in:
- Local: `pipeline_output/` or the `--output-dir` you provide
- S3: Under `s3://<bucket>/<key>/`

Partitioned outputs follow this structure:
```
pipeline_output/
  ├── region=NA/
  ├── region=EMEA/
  └── region=APAC/
```

---

## 🧹 Cleanup

```bash
docker compose down -v
rm -rf pipeline_output/
```

---

## 📓 Optional Preview

### With DuckDB
```bash
duckdb -c "SELECT COUNT(*) FROM 'pipeline_output/*.parquet'"
```

### With Jupyter Notebook
```bash
jupyter notebook preview.ipynb
```

---

## 📁 Files

- `run_pipeline.sh` — main driver script
- `extract_config.sh` — DB URL, SQL query, etc.
- `validate_output.sh` — data validators (row count, schema)
- `preview.ipynb` — optional visual preview

---

## 🚀 Next Phases

- Phase 2: Replace `bash` with Python orchestration
- Phase 3: Add Apache Airflow

