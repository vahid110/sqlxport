from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("DeltaWriterPartitioned") \
    .getOrCreate()

# Read from existing Parquet file
df = spark.read.parquet("sales.parquet")

# Write as Delta with partitioning by 'region'
df.write.format("delta") \
    .mode("overwrite") \
    .partitionBy("region") \
    .save("s3a://demo/delta_partitioned")

# Also write as plain CSV for compatibility
df.write.option("header", True).mode("overwrite").csv("s3a://demo/csv_output")

print("✅ Spark job completed with partitioned Delta write.")

# Read back from Delta partitioned output for verification
print("🔁 Verifying read from partitioned Delta Lake:")
df_read = spark.read.format("delta").load("s3a://demo/delta_partitioned")
df_read.groupBy("region").count().show()

spark.stop()
