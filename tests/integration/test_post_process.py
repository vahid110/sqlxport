# tests/integration/test_post_process.py

import os
import subprocess
import pytest


@pytest.fixture(scope="module")
def sample_partitioned_parquet(tmp_path_factory):
    base = tmp_path_factory.mktemp("partitioned_output")
    # Simulate a dummy partitioned output directory structure
    os.makedirs(base / "service=auth", exist_ok=True)
    os.makedirs(base / "service=search", exist_ok=True)
    with open(base / "service=auth" / "part-0001.parquet", "wb") as f:
        f.write(b"FAKE_PARQUET_DATA")
    with open(base / "service=search" / "part-0002.parquet", "wb") as f:
        f.write(b"FAKE_PARQUET_DATA")
    return base


def test_postprocess_touch_manifest_and_marker(sample_partitioned_parquet, temp_athena_database):
    ddl_path = sample_partitioned_parquet / "glue_table.sql"
    ddl_path.write_text(f"""
    CREATE EXTERNAL TABLE my_table (
      dummy STRING
    )
    PARTITIONED BY (service STRING)
    STORED AS PARQUET
    LOCATION 's3://vahidpersonal-east1/query-results/';
    """)

    result = subprocess.run([
        "sqlxport", "postprocess",
        "--athena-database", temp_athena_database,
        "--athena-output", "s3://vahidpersonal-east1/query-results/",
        "--athena-table-name", "my_table",
        "--region", "us-east-1",
        "--glue-register",
        "--repair-partitions",
        "--validate-table"
    ], cwd=str(sample_partitioned_parquet),
        capture_output=True, text=True
    )

    print("STDOUT:", result.stdout)
    print("STDERR:", result.stderr)
    assert result.returncode == 0


