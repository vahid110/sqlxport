import os
import uuid
import tempfile
import pytest
import pandas as pd
from sqlxport.core.storage import upload_file_to_s3
from sqlxport.utils.env import load_s3_config

@pytest.mark.skipif(
    not os.path.exists("tests/.env.test"),
    reason="Missing tests/.env.test config file"
)
def test_upload_file_to_s3_real():
    config = load_s3_config()

    # Create a dummy Parquet file
    df = pd.DataFrame({"name": ["Alice", "Bob"], "age": [30, 25]})
    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmpfile:
        df.to_parquet(tmpfile.name)
        local_path = tmpfile.name

    s3_key = f"test-upload/{uuid.uuid4()}.parquet"

    # Upload file to S3
    upload_file_to_s3(
        file_path=local_path,
        bucket_name=config["S3_BUCKET"],
        object_key=s3_key,
        access_key=config["AWS_ACCESS_KEY_ID"],
        secret_key=config["AWS_SECRET_ACCESS_KEY"],
        endpoint_url=config["S3_ENDPOINT_URL"],
        region_name=config["AWS_REGION"]
    )

    print(f"✅ Upload succeeded: s3://{config['S3_BUCKET']}/{s3_key}")
