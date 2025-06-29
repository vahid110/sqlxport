# tests/integration/test_preview_file_engines.py
import pytest
from click.testing import CliRunner
from dotenv import dotenv_values
from sqlxport.cli.cmd_preview import preview
import boto3
import uuid
import time
from pathlib import Path
import pandas as pd

config = dotenv_values("tests/.env.test")
runner = CliRunner()

@pytest.fixture(scope="module")
def sample_files():
    path = Path("tests/data")
    path.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame({"id": [1, 2], "name": ["Alice", "Bob"]})
    df.to_parquet(path / "sample.parquet", index=False)
    df.to_csv(path / "sample.csv", index=False)
    return {
        "parquet": str(path / "sample.parquet"),
        "csv": str(path / "sample.csv")
    }

@pytest.mark.integration
@pytest.mark.parametrize("format_", ["parquet", "csv"])
def test_preview_duckdb_local(sample_files, format_):
    file_path = sample_files[format_]

    result = runner.invoke(preview, [
        "--local-file", file_path,
        "--file-query-engine", "duckdb"
    ])

    assert result.exit_code == 0, result.output
    assert "Alice" in result.output or "| id" in result.output

@pytest.mark.integration
@pytest.mark.parametrize("format_, s3_prefix", [
    ("parquet", config["ATHENA_S3_PREFIX"]),
    ("csv", config["ATHENA_S3_PREFIX"].rstrip("/") + "_csv/")
])
def test_preview_athena_dynamic(sample_files, format_, s3_prefix):
    table_name = f"preview_{uuid.uuid4().hex[:8]}"
    s3_path = s3_prefix
    athena = boto3.client("athena", region_name=config["AWS_REGION"])
    s3 = boto3.client("s3", region_name=config["AWS_REGION"])
    bucket = config["S3_BUCKET"]
    local_file = sample_files[format_]
    s3_key = s3_path.replace(f"s3://{bucket}/", "") + Path(local_file).name

    # Upload test file to S3
    s3.upload_file(local_file, bucket, s3_key)

    # Compute parent path for LOCATION
    s3_path = f"s3://{bucket}/{s3_key.rsplit('/', 1)[0]}/"

    # Wait for S3 consistency
    for _ in range(10):
        try:
            s3.head_object(Bucket=bucket, Key=s3_key)
            break
        except s3.exceptions.ClientError:
            time.sleep(1)

    response = s3.list_objects_v2(Bucket=bucket, Prefix=s3_key.rsplit("/", 1)[0])
    print("S3 keys found:", [obj["Key"] for obj in response.get("Contents", [])])
    print("Uploaded:", s3_key)
    print("Using s3_path:", s3_path)
    print("Format:", format_)

    if format_ == "parquet":
        ddl = f"""
        CREATE EXTERNAL TABLE {config['ATHENA_DATABASE']}.{table_name} (
            id INT,
            name STRING
        )
        STORED AS PARQUET
        LOCATION '{s3_path}'
        """
    else:  # csv
        ddl = f"""
        CREATE EXTERNAL TABLE {config['ATHENA_DATABASE']}.{table_name} (
            id INT,
            name STRING
        )
        ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
        STORED AS TEXTFILE
        LOCATION '{s3_path}'
        TBLPROPERTIES ("skip.header.line.count"="1")
        """

    # Create table in Athena
    athena.start_query_execution(
        QueryString=ddl,
        QueryExecutionContext={"Database": config["ATHENA_DATABASE"]},
        ResultConfiguration={"OutputLocation": config["ATHENA_OUTPUT_LOCATION"]}
    )
    time.sleep(2)

    # Run preview using the same S3 path
    result = runner.invoke(preview, [
        "--local-file", local_file,
        "--file-query-engine", "athena",
        "--engine-args", f"database={config['ATHENA_DATABASE']}",
        "--engine-args", f"output_location={config['ATHENA_OUTPUT_LOCATION']}",
        "--engine-args", f"region={config['AWS_REGION']}",
        "--engine-args", f"s3_path={s3_path}",
        "--engine-args", f"file_format={format_}"
    ])




    assert result.exit_code == 0, result.output
    assert "Alice" in result.output or "| id" in result.output

    # Cleanup: drop table and delete uploaded file
    athena.start_query_execution(
        QueryString=f"DROP TABLE IF EXISTS {config['ATHENA_DATABASE']}.{table_name}",
        QueryExecutionContext={"Database": config["ATHENA_DATABASE"]},
        ResultConfiguration={"OutputLocation": config["ATHENA_OUTPUT_LOCATION"]}
    )
    s3.delete_object(Bucket=bucket, Key=s3_key)
