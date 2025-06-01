from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("TransformSalesData") \
    .getOrCreate()

print("✅ Spark session started")

# Load input data from MinIO
df = spark.read.parquet("s3a://demo-bucket/sales_data/")
df.printSchema()

# Simple transformation: filter
filtered = df.filter("amount > 1000")
print("✅ Filtered high-value transactions")

# Save back to MinIO
filtered.write.mode("overwrite").parquet("s3a://demo-bucket/sales_transformed/")

print("✅ Transformed data written to 'sales_transformed'")
