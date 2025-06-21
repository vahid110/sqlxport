# 🚀 sqlxport End-to-End Pipeline Demo

This demo extracts data from PostgreSQL, optionally uploads it to MinIO (S3-compatible), previews it with DuckDB, and transforms it with Spark.

## ▶️ How to Run

```bash
./run_sqlxport.sh --format parquet --partitioned --use-s3 --use-spark
```

## 🧩 Features
- Export to Parquet or CSV
- Partitioned and flat formats
- Upload to MinIO
- Preview with DuckDB
- Spark transformation (Phase 2)

## 🔧 Requirements
- Docker + Docker Compose
- sqlxport in PATH
- Optional: duckdb, pyspark

## 📂 Output
- `pipeline_output/`: extracted and transformed data
- MinIO bucket `demo-bucket/`

## 📜 License
MIT
