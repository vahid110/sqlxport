import subprocess
import pandas as pd
import pytest
import os

@pytest.fixture(scope="module")
def sample_parquet(tmp_path_factory):
    df = pd.DataFrame({
        "id": [1, 2, 3],
        "name": ["Alice", "Bob", "Charlie"],
        "amount": [10.5, 20.1, 30.7],
        "ts": pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-03"])
    })
    path = tmp_path_factory.mktemp("data") / "sample.parquet"
    df.to_parquet(path)
    return path

def test_generate_ddl_basic(sample_parquet):
    result = subprocess.run([
        "sqlxport", "generate-ddl",
        "--input-file", str(sample_parquet),
        "--file-query-engine", "duckdb",
        "--table-name", "my_table"
    ], capture_output=True, text=True)

    print(result.stdout)
    print(result.stderr)
    assert result.returncode == 0


