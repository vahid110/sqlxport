# sql2data + Spark + Delta Lake with MinIO Demo

This demo shows how to extract data from PostgreSQL using `sql2data`, upload it as Parquet to MinIO (S3-compatible), then read and convert it to Delta Lake using Apache Spark.

---

## 🔧 Requirements

- Docker & Docker Compose
- Python 3 with `sql2data` and `pyspark` installed
- (Optional) `duckdb` or Jupyter for local preview

---

## ▶️ Steps

### 1. Start and Seed

```bash
chmod +x run_sql2data.sh
./run_sql2data.sh
```

This:
- Spins up PostgreSQL + MinIO via Docker
- Seeds a `sales` table
- Uses `sql2data` to export it to MinIO as Parquet

### 2. Convert to Delta Lake

```bash
chmod +x run_spark_in_docker.sh
./run_spark_in_docker.sh
```

This:
- Runs PySpark with Delta Lake extensions
- Loads the Parquet files from MinIO (S3)
- Writes the Delta Lake output to `delta_output/`

---

## 🔍 Verifying Output

You can inspect Delta output via:

### DuckDB fallback:

```bash
duckdb -c "SELECT * FROM 'delta_output/*.parquet' LIMIT 10"
```

### Spark shell (inside container):

```bash
docker compose exec spark-runner spark-shell \
  --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
  --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"

val df = spark.read.format("delta").load("delta_output")
df.show()
```

---

## 🧼 Cleanup

```bash
docker compose down -v
rm -rf delta_output/
```

---

## 🧠 Notes

- You can inspect MinIO at http://localhost:9001 (user: `admin`, password: `password`)
- Bucket: `sql2data-bucket`, Prefix: `parquet`
- This demo is self-contained and runs entirely locally.
