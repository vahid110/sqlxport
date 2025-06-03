# Spark + Delta Lake Demo with MinIO and PostgreSQL

This demo showcases how to integrate `sql2data` with a Spark + Delta Lake pipeline using PostgreSQL and MinIO.

## 🧱 Architecture

- **PostgreSQL**: Stores the source `sales` table.
- **sql2data**: Exports SQL query results to a Parquet file.
- **Spark**: Reads the Parquet file, writes data to:
  - Delta Lake format (unpartitioned + partitioned)
  - CSV format
- **MinIO**: Serves as an S3-compatible object store.
- **mc**: Used for validating S3 contents.
- **DuckDB**: Previews Delta outputs locally.

## 🚀 How to Run

```bash
cd demo/spark_minio_delta
./run_sql2data.sh
```

This will:
1. Launch all services via Docker Compose.
2. Seed the PostgreSQL database.
3. Export `sales` table to `sales.parquet`.
4. Submit a Spark job that:
   - Writes Delta to `s3a://demo/delta_output`
   - Writes partitioned Delta to `s3a://demo/delta_partitioned`
   - Writes CSV to `s3a://demo/csv_output`
   - Reads Delta back for verification.
5. Lists contents of each S3 output folder.
6. (Optional) Previews partitioned Delta with DuckDB.

## 📦 Requirements

- Docker + Docker Compose
- `sql2data` installed (e.g., `pip install -e .`)
- (Optional) `mc` (MinIO client) for output inspection
- (Optional) `duckdb` CLI for Delta preview

## 🔍 Sample Output

```text
📄 Delta partition folders in 'demo/delta_partitioned/':
- region=EMEA/
- region=NA/
- region=APAC/

+------+-----+
|region|count|
+------+-----+
|    NA|    5|
|  APAC|    5|
|  EMEA|    5|
+------+-----+
```

## 📁 Files

- `run_sql2data.sh`: Main driver script.
- `Dockerfile.spark`: Custom Spark container with JARs.
- `docker-compose.yml`: Services for demo.
- `run_spark_query.py`: Spark job to write + read Delta/CSV.
