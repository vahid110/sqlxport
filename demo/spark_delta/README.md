# 🧪 sql2data + Spark + Delta Lake Demo

This demo shows how to extract data from PostgreSQL using [`sql2data`](https://github.com/vahid110/sql2data), convert it to [Delta Lake](https://delta.io) format using Apache Spark, and query it via Spark SQL — all locally and using only open source tools.

---

## 🚀 Overview

- Extract data from PostgreSQL to Parquet using `sql2data`
- Convert Parquet to Delta Lake using Apache Spark
- Query Delta Lake with Spark SQL
- Optional previews with DuckDB or Jupyter Notebook

---

## 🔧 Requirements

- [Docker](https://docs.docker.com/get-docker/) + Docker Compose
- Python 3 (with `pyspark` installed)
- `sql2data` CLI installed and available in your `$PATH`
- (Optional) [`duckdb`](https://duckdb.org/) or `jupyter`

Install PySpark if needed:

```bash
pip install pyspark
```

---

## ▶️ How to Run

### Step 1: Start Services

```bash
docker compose up -d
```

Wait ~10 seconds for PostgreSQL to initialize.

---

### Step 2: Run the Demo

```bash
chmod +x run_sql2data.sh
./run_sql2data.sh
```

This script will:
- Create and seed a PostgreSQL demo database
- Export data to Parquet with `sql2data`
- Run a Spark job to convert and query Delta Lake format
- Print a DuckDB preview command

---

### Step 3: Optional Preview

#### Preview Parquet with DuckDB

```bash
duckdb -c "SELECT * FROM 'sales.parquet' LIMIT 5;"
```

#### Preview in Jupyter Notebook

```bash
jupyter notebook preview.ipynb
```

---

## ✅ Verifying Delta Output

DuckDB **cannot read Delta metadata** directly. To verify the Delta output:

### Option 1: Preview Delta output with Spark shell

```bash
docker compose exec spark-runner spark-shell   --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension"   --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"
```

Then inside the Spark shell:

```scala
val df = spark.read.format("delta").load("delta_output")
df.show()
```

### Option 2: DuckDB fallback (reads Parquet files only)

```bash
duckdb -c "SELECT * FROM 'delta_output/*.parquet' LIMIT 10"
```

---

## 🧼 Cleanup

```bash
docker compose down -v
rm -rf delta_output/ sales.parquet
```

---

## 🧠 Notes

- This demo uses Delta Lake's file-based transactional log format via PySpark.
- You can extend this to simulate schema evolution, versioning, or time travel.
- A future variant of this demo will include cloud-style storage with MinIO.

---

Enjoy exploring your local data lake! 🌊
