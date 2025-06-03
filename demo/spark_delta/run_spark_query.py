# demo/spark_delta/run_spark_query.py

from pyspark.sql import SparkSession
import os
from pathlib import Path

# Start Spark session with Delta support
spark = SparkSession.builder \
    .appName("sqlxport Spark Demo") \
    .master("local[*]") \
    .config("spark.driver.host", "127.0.0.1") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Load Parquet
df = spark.read.parquet("sales.parquet")
df.show()

# Convert to Delta (partitioned)
df.write.format("delta").mode("overwrite").partitionBy("region").save("delta_output")
print("✅ Delta Lake table created at delta_output/")

# Query Delta
delta_df = spark.read.format("delta").load("delta_output")

print("✅ Sum of amount per region:")
delta_df.groupBy("region").sum("amount").show()

print("✅ Previewing 5 rows from the Delta table:")
delta_df.show(5)

print("✅ Row count per region:")
delta_df.groupBy("region").count().show()

print("✅ Total row count:")
print(delta_df.count())

print("✅ Distinct regions:")
delta_df.select("region").distinct().show()

# Also write as CSV
csv_path = "csv_output"
df.coalesce(1).write.mode("overwrite").option("header", "true").csv(csv_path)
print(f"✅ CSV written to: {csv_path}/")

# Optional: Load and preview CSV output
csv_df = spark.read.option("header", "true").csv(csv_path)
print("✅ Preview from CSV output:")
csv_df.show(5)

# Validate Delta partitions
delta_path = "delta_output"
partitions = [p for p in Path(delta_path).iterdir() if p.is_dir() and p.name.startswith("region=")]
if partitions:
    print(f"\n✅ Found {len(partitions)} region partitions in {delta_path}/:")
    for p in partitions:
        print(f"  - {p.name}")
else:
    print("⚠️ No region partitions found in delta_output/")

# Validate CSV files
csv_files = list(Path(csv_path).rglob("*.csv"))
if csv_files:
    print(f"\n✅ Found {len(csv_files)} CSV file(s) in {csv_path}/:")
    for f in csv_files:
        size_kb = f.stat().st_size / 1024
        print(f"  - {f.name} ({size_kb:.1f} KB)")
else:
    print("⚠️ No CSV files found in csv_output/")

# Done
spark.stop()
