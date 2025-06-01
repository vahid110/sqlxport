import sys
import os
from pyspark.sql import SparkSession

input_path = sys.argv[1]
partitioned = "--partitioned" in sys.argv
output_path = None

if "--output-path" in sys.argv:
    idx = sys.argv.index("--output-path")
    output_path = sys.argv[idx + 1]
else:
    output_path = "delta_output"

spark = (
    SparkSession.builder.appName("Delta Lake Conversion")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

print(f"📥 Reading from: {input_path}")
spark.conf.set("spark.sql.sources.partitionDiscovery.enabled", "true")

# Handle partitioned mode and check for optional 'data' subdir
from py4j.java_gateway import java_import
java_import(spark._jvm, "org.apache.hadoop.fs.Path")

hadoop_conf = spark._jsc.hadoopConfiguration()
data_path = spark._jvm.Path(input_path + "/data")
fs = data_path.getFileSystem(hadoop_conf)
use_data_subdir = fs.exists(data_path)

load_path = os.path.join(input_path, "data") if use_data_subdir else input_path

reader = spark.read.format("parquet")

if partitioned:
    reader = reader.option("basePath", input_path)

df = reader.load(load_path)

print("🧪 Previewing data:")
df.show()

print(f"💾 Writing Delta to: {output_path}")
writer = df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")

if partitioned:
    print("📂 Partitioning by region...")
    writer = writer.partitionBy("region")

writer.save(output_path)

print("✅ Done.")
