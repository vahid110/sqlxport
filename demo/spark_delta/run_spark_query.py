from pyspark.sql import SparkSession

# Start Spark session with Delta support
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("SQL2Data Spark Demo") \
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

# Convert to Delta
df.write.format("delta").mode("overwrite").save("delta_output")
print("✅ Delta Lake table created at delta_output/")

# Query Delta
delta_df = spark.read.format("delta").load("delta_output")
delta_df.groupBy("region").sum("amount").show()

spark.stop()

