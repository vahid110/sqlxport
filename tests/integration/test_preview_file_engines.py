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
def test_preview_athena_dynamic(format_, s3_prefix):
    table_name = f"preview_{uuid.uuid4().hex[:8]}"
    s3_path = s3_prefix
    athena = boto3.client("athena", region_name=config["AWS_REGION"])

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

    # Create table
    athena.start_query_execution(
        QueryString=ddl,
        QueryExecutionContext={"Database": config["ATHENA_DATABASE"]},
        ResultConfiguration={"OutputLocation": config["ATHENA_OUTPUT_LOCATION"]}
    )
    time.sleep(2)

    # Run preview
    result = runner.invoke(preview, [
        "--local-file", s3_path,
        "--file-query-engine", "athena",
        "--engine-args", f"database={config['ATHENA_DATABASE']}",
        "--engine-args", f"output_location={config['ATHENA_OUTPUT_LOCATION']}",
        "--engine-args", f"region={config['AWS_REGION']}"
    ])
    assert result.exit_code == 0, result.output
    assert "Alice" in result.output or "| id" in result.output

    # Cleanup
    athena.start_query_execution(
        QueryString=f"DROP TABLE IF EXISTS {config['ATHENA_DATABASE']}.{table_name}",
        QueryExecutionContext={"Database": config["ATHENA_DATABASE"]},
        ResultConfiguration={"OutputLocation": config["ATHENA_OUTPUT_LOCATION"]}
    )
