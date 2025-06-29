# tests/integration/test_generate_ddl_coverages.py

import subprocess
import pytest
from pathlib import Path
import shutil


@pytest.fixture
def tmp_csv_file(tmp_path):
    csv_path = tmp_path / "sample.csv"
    csv_path.write_text("id,name\n1,Alice\n2,Bob\n3,Charlie\n")
    return csv_path


@pytest.fixture
def tmp_parquet_file(tmp_path):
    import pandas as pd
    df = pd.DataFrame({"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"]})
    pq_path = tmp_path / "sample.parquet"
    df.to_parquet(pq_path)
    return pq_path


def run_ddl_cmd(input_file, partition_by=None):
    cmd = [
        "sqlxport", "generate-ddl",
        "--input-file", str(input_file),
        "--output-format", "athena",
        "--file-query-engine", "duckdb",
        "--table-name", "test_table",
        "--s3-url", "s3://dummy-bucket/output/",
    ]

    if partition_by:
        cmd += ["--partition-by", partition_by]

    result = subprocess.run(cmd, capture_output=True, text=True)
    # 🔍 Always print output for debugging
    print("--- STDOUT ---")
    print(result.stdout)
    print("--- STDERR ---")
    print(result.stderr)
    return result




def test_generate_ddl_parquet(tmp_parquet_file):
    result = run_ddl_cmd(tmp_parquet_file)
    assert result.returncode == 0
    assert "CREATE EXTERNAL TABLE" in result.stdout


def test_generate_ddl_csv(tmp_csv_file):
    tmp_csv_file.write_text("id,name,email\n1,John,john@example.com\n2,Jane,jane@example.com")
    result = run_ddl_cmd(tmp_csv_file)
    assert result.returncode == 0
    assert "CREATE EXTERNAL TABLE" in result.stdout

def test_generate_ddl_with_partition(tmp_parquet_file):
    result = run_ddl_cmd(tmp_parquet_file, partition_by="name")
    assert result.returncode == 0
    ddl = result.stdout
    # Adjusted to match generated formatting
    assert "PARTITIONED BY (\n  name STRING" in ddl


def test_generate_ddl_nonexistent_file(tmp_path):
    fake_path = tmp_path / "does_not_exist.parquet"
    result = run_ddl_cmd(fake_path)
    assert result.returncode != 0
    assert "No such file" in result.stderr or "IO Error" in result.stderr or "Traceback" in result.stderr


def test_generate_ddl_invalid_format(tmp_path):
    junk_file = tmp_path / "junk.txt"
    junk_file.write_text("just some text")
    result = run_ddl_cmd(junk_file)
    assert result.returncode != 0
    assert (
        "IO Error" in result.stderr
        or "parsing error" in result.stderr.lower()
        or "Traceback" in result.stderr
    )
