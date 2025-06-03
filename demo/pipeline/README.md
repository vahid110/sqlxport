# sqlxport Demo Pipeline

This demo showcases how to export data from a PostgreSQL database to different formats (Parquet or CSV), optionally partitioned by a column (e.g., `region`), and optionally upload the result to MinIO (S3-compatible storage). It also provides optional preview functionality using DuckDB.

---

## 🧩 Features
- Export from PostgreSQL to:
  - Flat Parquet
  - Partitioned Parquet
  - Flat CSV
  - Partitioned CSV
- Optional upload to MinIO (S3-compatible)
- Optional preview with DuckDB (row count, partition distribution)
- Optional Spark processing phase (can be enabled)

---

## 🏗️ Setup

1. Make sure PostgreSQL and MinIO services are running (can be launched via Docker).
2. Activate the Python virtual environment if not already active.
3. Install DuckDB CLI for preview functionality (optional):
   ```bash
   brew install duckdb  # or download from https://duckdb.org
   ```
---

## 🛠️ Developer Notes
- `run_pipeline.sh` orchestrates extraction, saving, and previewing.
- `run_full_pipeline.sh` additionally triggers Spark Delta Lake transformation.
- Internally uses `sqlxport run` CLI (`write_flat` or `write_partitioned`).
- S3 uploads done via Dockerized `mc` tool.

### 🔄 Internal Pipeline Stages

| Stage     | Description                                     | Tool         |
|-----------|-------------------------------------------------|--------------|
| Extract   | Run SQL query and fetch results from PostgreSQL | `sqlxport`   |
| Transform | Convert to Parquet or CSV (flat or partitioned) | `sqlxport`   |
| Validate  | Preview output using row counts or partitioning | `duckdb`     |
| Load      | Upload to MinIO (if enabled)                    | `mc` (MinIO) |
| Delta     | (Optional) Spark transformation to Delta Lake   | `spark-submit` |

---

## 🚀 Usage Examples

### 1. Flat Parquet (local only)
```bash
./run_pipeline.sh --format parquet --output-dir flat_parquet
```

### 2. Partitioned Parquet (local only)
```bash
./run_pipeline.sh --partitioned --format parquet --output-dir sales_partitioned_parquet
```

### 3. Flat CSV (local only)
```bash
./run_pipeline.sh --format csv --output-dir flat_csv
```

### 4. Partitioned CSV (local only)
```bash
./run_pipeline.sh --partitioned --format csv --output-dir sales_csv
```

### 5. Partitioned Parquet + Upload to S3
```bash
./run_pipeline.sh --partitioned --format parquet --output-dir parquet_s3 --use-s3
```

### 6. Flat Parquet + Upload to S3
```bash
./run_pipeline.sh --format parquet --output-dir flat_parquet --use-s3
```

### 7. Disable Preview
```bash
./run_pipeline.sh --format parquet --no-preview
```

### 8. Enable Spark Phase (experimental)
```bash
./run_pipeline.sh --partitioned --format parquet --use-spark
```

---

## 📂 Output Structure

- Flat:
  ```
  output_dir/
    └── output.parquet or output.csv
  ```
- Partitioned:
  ```
  output_dir/
    ├── region=NA/
    │   └── part-0000.parquet or .csv
    ├── region=EMEA/
    │   └── part-0000.parquet or .csv
    └── region=APAC/
        └── part-0000.parquet or .csv
  ```

---

## 📊 Preview with DuckDB
- Automatically enabled unless `--no-preview` is passed.
- Shows either total row count (flat) or partitioned counts (if partitioned output).

---

## 🛠️ Internal Notes (for developers)
- Output is saved using `sqlxport run` CLI.
- Uses `write_flat` or `write_partitioned` internally depending on arguments.
- Phase 2 processing (Spark) is optional and kicked off via `--use-spark`.
- S3 upload handled via MinIO CLI (`mc`) in Docker.

---

✅ Done! You can now explore `sqlxport` end-to-end locally or with S3/Spark integrations.
