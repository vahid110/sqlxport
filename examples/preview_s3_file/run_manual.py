import boto3
import pyarrow.parquet as pq
import io

bucket = "my-bucket"
key = "users.parquet"

s3 = boto3.client("s3")
response = s3.get_object(Bucket=bucket, Key=key)

data = response["Body"].read()
table = pq.read_table(io.BytesIO(data))

print(table.to_pandas().head())