# tests/integration/test_generate_ddl.py
import subprocess
import pandas as pd
import pytest
import os

@pytest.fixture(scope="module")
def sample_parquet(tmp_path_factory):
    import pyarrow as pa
    import pyarrow.parquet as pq

    df = pd.DataFrame({
        "id": [1, 2, 3],
        "name": ["Alice", "Bob", "Charlie"],
        "amount": [10.5, 20.1, 30.7],
        "ts": pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-03"])
    })
    table = pa.Table.from_pandas(df)
    path = tmp_path_factory.mktemp("data") / "sample.parquet"
    pq.write_table(table, path)
    return path

def test_generate_ddl_basic(sample_parquet):
    result = subprocess.run([
        "sqlxport", "generate-ddl",
        "--input-file", str(sample_parquet),
        "--file-query-engine", "duckdb",
        "--table-name", "my_table",
        "--s3-url", "s3://dummy-bucket/path/"
    ], capture_output=True, text=True)

    print("STDOUT:", result.stdout)
    print("STDERR:", result.stderr)

    ddl = result.stdout
    assert result.returncode == 0
    assert "CREATE EXTERNAL TABLE" in ddl
    assert "my_table" in ddl
    assert "id BIGINT" in ddl
    assert "name STRING" in ddl
    assert "amount" in ddl
    assert any(dtype in ddl for dtype in [
        "amount DOUBLE", "amount DOUBLE PRECISION", "amount FLOAT", "amount STRING"
    ])
    assert "ts TIMESTAMP" in ddl


def test_generate_ddl_missing_args():
    result = subprocess.run([
        "sqlxport", "generate-ddl"
    ], capture_output=True, text=True)

    print("STDOUT:", result.stdout)
    print("STDERR:", result.stderr)
    assert result.returncode != 0
    assert "Missing option" in result.stderr or "Error" in result.stderr

def test_generate_ddl_partitioned(sample_parquet):
    result = subprocess.run([
        "sqlxport", "generate-ddl",
        "--input-file", str(sample_parquet),
        "--file-query-engine", "duckdb",
        "--table-name", "my_partitioned_table",
        "--s3-url", "s3://dummy-bucket/partitioned/",
        "--partition-by", "name"
    ], capture_output=True, text=True)

    print("STDOUT:", result.stdout)
    print("STDERR:", result.stderr)

    ddl = result.stdout
    assert result.returncode == 0
    assert "CREATE EXTERNAL TABLE" in ddl
    assert "my_partitioned_table" in ddl
    import re
    assert re.search(r"PARTITIONED BY\s*\(\s*name\s+STRING\s*\)", ddl)
    assert "STORED AS PARQUET" in ddl
    assert "LOCATION 's3://dummy-bucket/partitioned/'" in ddl

def test_generate_ddl_invalid_output_format(sample_parquet):
    result = subprocess.run([
        "sqlxport", "generate-ddl",
        "--input-file", str(sample_parquet),
        "--file-query-engine", "duckdb",
        "--output-format", "hive",  # Unsupported on purpose
        "--table-name", "bad_table",
        "--s3-url", "s3://dummy/"
    ], capture_output=True, text=True)

    print("STDOUT:", result.stdout)
    print("STDERR:", result.stderr)

    assert result.returncode != 0
    assert "DDL format 'hive' is not supported" in result.stderr

def test_generate_ddl_multiple_partitions(sample_parquet):
    result = subprocess.run([
        "sqlxport", "generate-ddl",
        "--input-file", str(sample_parquet),
        "--file-query-engine", "duckdb",
        "--table-name", "multi_part_table",
        "--s3-url", "s3://dummy/multi/",
        "--partition-by", "name,ts"
    ], capture_output=True, text=True)

    print("STDOUT:", result.stdout)
    print("STDERR:", result.stderr)

    ddl = result.stdout
    assert result.returncode == 0
    assert "CREATE EXTERNAL TABLE" in ddl
    assert "multi_part_table" in ddl
    assert "PARTITIONED BY" in ddl
    assert "name STRING" in ddl
    assert "ts TIMESTAMP" in ddl

    # Optional: stronger format check
    assert "PARTITIONED BY (\n  name STRING,\n  ts TIMESTAMP\n)" in ddl or \
           "PARTITIONED BY (name STRING, ts TIMESTAMP)" in ddl
