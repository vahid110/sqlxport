# 🧪 sqlxport + Spark + Delta Lake Demo

This demo shows how to extract data from PostgreSQL using [`sqlxport`](https://github.com/vahid110/sqlxport), convert it to [Delta Lake](https://delta.io) and CSV format using Apache Spark, and query the results — all locally using only open source tools.

---

## 🚀 Overview

- Extract data from PostgreSQL to Parquet using `sqlxport`
- Convert Parquet to **partitioned Delta Lake** using Apache Spark
- Export same data as CSV (with header)
- Query Delta and CSV outputs via Spark SQL
- Optional preview using DuckDB or Jupyter Notebook

---

## 🔧 Requirements

- [Docker](https://docs.docker.com/get-docker/) + Docker Compose
- Python 3 (with `pyspark` installed)
- `sqlxport` CLI installed and available in your `$PATH`
- (Optional) [`duckdb`](https://duckdb.org/) or `jupyter`

Install PySpark if needed:

```bash
pip install pyspark
```

---

## ▶️ How to Run

### Step 1: Start Services and Run Full Pipeline

```bash
./run_sqlxport.sh
```

This will:
- Start PostgreSQL and Spark services
- Create and seed a demo `sales` table in PostgreSQL
- Export the table to `sales.parquet` via `sqlxport`
- Run a Spark job that:
  - Writes a **partitioned Delta Lake table** to `delta_output/`
  - Writes a **CSV export** to `csv_output/`
  - Previews both via Spark
  - Validates outputs (partition folders, CSV files)

---

## 🔍 Optional Preview

### Preview Parquet with DuckDB

```bash
duckdb -c "SELECT * FROM 'sales.parquet' LIMIT 5;"
```

### Preview Delta fallback with DuckDB (just Parquet files)

```bash
duckdb -c "SELECT * FROM glob('delta_output/region=*/**/*.parquet') LIMIT 10;"
```

### Preview CSV manually

```bash
duckdb -c "SELECT * FROM glob('csv_output/*.csv') LIMIT 5;"
```

### Preview via Jupyter (experimental – ensure Delta JAR is on Spark classpath)

```bash
jupyter notebook preview.ipynb
```

---

## ✅ Output Structure

- `sales.parquet` — exported with `sqlxport`
- `delta_output/region=.../` — partitioned Delta table
- `csv_output/part-*.csv` — coalesced CSV file with header

---

## 🧼 Cleanup

```bash
docker compose down -v
rm -rf delta_output/ csv_output/ sales.parquet
```

---

## 🛠️ Troubleshooting

### PostgreSQL port 5432 already in use?

You may see this error:

```
Bind for 0.0.0.0:5432 failed: port is already allocated
```

It means another process is using PostgreSQL's default port. To resolve:

```bash
docker ps | grep 5432
docker stop <container_id>
```

Then re-run the demo.

---

### Delta SparkSessionExtension Not Found?

If you see:

```
ClassNotFoundException: io.delta.sql.DeltaSparkSessionExtension
```

Make sure your Spark session includes the Delta Lake JARs. This is not yet supported in the default Jupyter Python kernel.

---

## 🧠 Notes

- This demo simulates a real-world data lake ingestion and conversion pipeline.
- You can adapt this to S3 + MinIO or extend it with schema evolution & time travel.
- Fully self-contained — ideal for testing locally.

---

Enjoy exploring your local data lake! 🌊
