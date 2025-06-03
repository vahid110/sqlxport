# sqlxport/core/storage.py
import boto3
import pandas as pd
import io
import os
import pyarrow.parquet as pq
import posixpath 

def upload_file_to_s3(
    file_path,
    bucket_name,
    object_key,
    access_key,
    secret_key,
    endpoint_url="http://localhost:9000",
    region_name="us-east-1"
):
    s3 = boto3.client(
        's3',
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        endpoint_url=endpoint_url,
        region_name=region_name
    )

    s3.upload_file(file_path, bucket_name, object_key)
    print(f"✅ Uploaded to s3://{bucket_name}/{object_key}")

def list_s3_objects(bucket, prefix, access_key, secret_key, endpoint_url="https://s3.amazonaws.com", region_name="us-east-1"):
    s3 = boto3.client(
        's3',
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        endpoint_url=endpoint_url,
        region_name=region_name
    )

    print(f"📂 Listing s3://{bucket}/{prefix}")
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)

    contents = response.get("Contents", [])
    if not contents:
        print("⚠️ No files found.")
        return

    for obj in contents:
        print(f"📄 {obj['Key']} ({obj['Size']} bytes)")

def preview_s3_parquet(bucket, key, access_key, secret_key, endpoint_url="https://s3.amazonaws.com", region_name="us-east-1", max_rows=10):
    s3 = boto3.client(
        's3',
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        endpoint_url=endpoint_url,
        region_name=region_name
    )

    print(f"📥 Downloading s3://{bucket}/{key}")
    obj = s3.get_object(Bucket=bucket, Key=key)
    buffer = io.BytesIO(obj['Body'].read())

    df = pd.read_parquet(buffer)
    print(f"🧾 Preview of {key} (first {max_rows} rows):\n")
    print(df.head(max_rows))

def preview_local_parquet(file_path: str, max_rows: int = 10):
    try:
        df = pd.read_parquet(file_path)
        print(f"🧾 Preview of {file_path} (first {max_rows} rows):\n")
        print(df.head(max_rows).to_markdown(index=False))
    except Exception as e:
        print(f"❌ Failed to read {file_path}: {e}")

def upload_folder_to_s3(local_dir, bucket, prefix, access_key, secret_key, endpoint_url=None, region_name="us-east-1"):
    session = boto3.session.Session(
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key
    )
    s3 = session.client('s3', endpoint_url=endpoint_url, region_name=region_name)

    for root, _, files in os.walk(local_dir):
        for filename in files:
            local_path = os.path.join(root, filename)
            relative_path = os.path.relpath(local_path, local_dir)
            # 🔧 Safe S3 key using posix-style joining
            s3_key = posixpath.join(prefix or '', relative_path)

            print(f"☁️ Uploading {relative_path} to s3://{bucket}/{s3_key}")
            s3.upload_file(local_path, bucket, s3_key)

    print("✅ Folder upload complete.")
