import uuid
import configparser
import os
import pytest
import boto3
from urllib.parse import urlparse
from sqlxport.redshift_unload import run_unload
from sqlxport.utils.env import load_env_file

@pytest.mark.skipif(
    not os.path.exists("tests/.env.test"),
    reason="Missing tests/.env.test config file"
)
def test_run_unload_real():
    env = load_env_file("tests/.env.test")
    db_url = env["REDSHIFT_DB_URL"]
    iam_role = env["REDSHIFT_IAM_ROLE"]

    s3_prefix = f"s3://{env['S3_BUCKET']}/unload-test/{uuid.uuid4()}/"

    run_unload(
        db_url=db_url,
        query="SELECT 1 AS test_col",
        s3_output_prefix=s3_prefix,
        iam_role=iam_role
    )

    parsed = urlparse(s3_prefix)
    s3 = boto3.client(
        "s3",
        aws_access_key_id=env["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=env["AWS_SECRET_ACCESS_KEY"],
        region_name=env.get("AWS_REGION", "us-east-1"),
        endpoint_url=env.get("S3_ENDPOINT_URL")
    )
    response = s3.list_objects_v2(Bucket=parsed.netloc, Prefix=parsed.path.lstrip("/"))
    contents = response.get("Contents", [])
    assert any(obj["Key"].endswith(".parquet") for obj in contents), "No .parquet file found in S3 output"

    print(f"✅ Redshift UNLOAD succeeded: {len(contents)} file(s) written to {s3_prefix}")
