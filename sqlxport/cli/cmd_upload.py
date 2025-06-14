import os
import posixpath
import click

from sqlxport.core.storage import upload_file_to_s3
from sqlxport.core.s3_config import S3Config


@click.command("upload")
@click.option("--source-dir", required=True, help="Path to local directory to upload")
@click.option("--s3-bucket", required=True, help="S3 bucket name")
@click.option("--s3-key", required=True, help="S3 key/prefix to upload into")
@click.option("--s3-access-key", required=True, help="S3 access key")
@click.option("--s3-secret-key", required=True, help="S3 secret key")
@click.option("--s3-endpoint", required=False, help="S3 endpoint URL")
@click.option("--aws-region", default="us-east-1", help="AWS region name")
def upload(source_dir, s3_bucket, s3_key, s3_access_key, s3_secret_key, s3_endpoint, aws_region):
    """Upload a local directory to S3 (recursively)"""
    print("☁️ Uploading directory recursively to S3...")

    s3_cfg = S3Config(
        bucket=s3_bucket,
        key=s3_key,
        access_key=s3_access_key,
        secret_key=s3_secret_key,
        endpoint_url=s3_endpoint,
        region_name=aws_region
    )

    for root, _, files in os.walk(source_dir):
        for file in files:
            full_path = os.path.join(root, file)
            rel_path = os.path.relpath(full_path, source_dir)
            object_key = posixpath.join(s3_key, rel_path)
            print(f"   • {rel_path}")
            upload_file_to_s3(
                file_path=full_path,
                bucket_name=s3_bucket,
                object_key=object_key,
                access_key=s3_access_key,
                secret_key=s3_secret_key,
                endpoint_url=s3_endpoint,
                region_name=aws_region
            )

    print("✅ Folder upload complete.")
