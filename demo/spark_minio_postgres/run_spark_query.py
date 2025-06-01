from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.appName("Delta Lake Conversion")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

print("📥 Reading Parquet files from MinIO (via S3)...")
df = spark.read.format("parquet").load("s3a://demo-bucket/sales/sales.parquet")

print("🧪 Previewing data:")
df.show()

print("💾 Writing as Delta Lake format to local folder...")
df.write.format("delta").mode("overwrite").save("delta_output")

print("✅ Done.")
