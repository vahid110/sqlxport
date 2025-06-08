# tests/integration/test_validate_table.py

import subprocess
import pytest
from dotenv import load_dotenv
from pathlib import Path
import os
import boto3
import time
import uuid
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import tempfile

# Load test environment variables
load_dotenv(dotenv_path=Path("tests/.env.test"))

ATHENA_DATABASE = os.getenv("ATHENA_DATABASE", "default")
ATHENA_OUTPUT = os.getenv("ATHENA_OUTPUT_LOCATION")
S3_BUCKET = os.getenv("S3_BUCKET")
S3_PREFIX = os.getenv("ATHENA_S3_PREFIX", f"s3://{S3_BUCKET}/sample/")

@pytest.fixture(scope="session")
def parquet_data_uploaded():
    # Step 1: Generate a simple Parquet file
    df = pd.DataFrame({
        "id": [1, 2, 3],
        "name": ["Alice", "Bob", "Charlie"]
    })

    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
        table = pa.Table.from_pandas(df)
        pq.write_table(table, tmp.name)
        local_parquet_path = tmp.name

    # Step 2: Upload to S3
    s3 = boto3.client("s3", region_name="us-east-1")
    object_key = "sample/sample.parquet"
    s3.upload_file(local_parquet_path, S3_BUCKET, object_key)

    return f"s3://{S3_BUCKET}/sample/sample.parquet"

@pytest.fixture
def temp_athena_table(parquet_data_uploaded):
    table_name = f"test_table_{uuid.uuid4().hex[:8]}"
    create_query = f"""
    CREATE EXTERNAL TABLE {table_name} (
        id INT,
        name STRING
    )
    STORED AS PARQUET
    LOCATION '{S3_PREFIX}'
    """

    athena = boto3.client("athena", region_name="us-east-1")
    response = athena.start_query_execution(
        QueryString=create_query,
        QueryExecutionContext={"Database": ATHENA_DATABASE},
        ResultConfiguration={"OutputLocation": ATHENA_OUTPUT}
    )

    exec_id = response["QueryExecutionId"]

    while True:
        status = athena.get_query_execution(QueryExecutionId=exec_id)["QueryExecution"]["Status"]["State"]
        if status in {"SUCCEEDED", "FAILED"}:
            break
        time.sleep(1)

    if status != "SUCCEEDED":
        reason = athena.get_query_execution(QueryExecutionId=exec_id)["QueryExecution"]["Status"].get("StateChangeReason", "")
        raise RuntimeError(f"CREATE TABLE failed: {reason}")

    yield table_name

    # Teardown
    drop_query = f"DROP TABLE IF EXISTS {table_name}"
    athena.start_query_execution(
        QueryString=drop_query,
        QueryExecutionContext={"Database": ATHENA_DATABASE},
        ResultConfiguration={"OutputLocation": ATHENA_OUTPUT}
    )

def test_validate_table_athena(temp_athena_table):
    result = subprocess.run([
        "sqlxport", "validate-table",
        "--table-name", temp_athena_table,
        "--file-query-engine", "athena",
        "--athena-output-location", ATHENA_OUTPUT
    ], capture_output=True, text=True)

    print(result.stdout)
    print(result.stderr)

    assert result.returncode == 0
    assert "Athena table is queryable" in result.stdout
