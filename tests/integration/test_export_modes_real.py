# tests/integration/test_export_modes_real.py

import uuid
import os
import pytest
import boto3
from urllib.parse import urlparse
from click.testing import CliRunner
from sqlxport.cli.main import run
from sqlxport.utils.env import load_env_file

os.environ["SQLXPORT_ENV_PATH"] = "tests/.env.test"
env = load_env_file("tests/.env.test")

@pytest.mark.skipif("REDSHIFT_DB_URL" not in env, reason="No Redshift config in .env.test")
def test_redshift_unload_via_export_mode():
    db_url = env["REDSHIFT_DB_URL"]
    iam_role = env["REDSHIFT_IAM_ROLE"]
    s3_prefix = f's3://{env["S3_BUCKET"]}/test-export-mode-unload/{uuid.uuid4()}'

    result = CliRunner().invoke(run, [
        "--export-mode", "redshift-unload",
        "--db-url", db_url,
        "--query", "SELECT 1 AS test_col",
        "--iam-role", iam_role,
        "--s3-output-prefix", s3_prefix
    ])

    assert result.exit_code == 0, f"Run failed: {result.output}"

    # ✅ Check that a Parquet file was uploaded
    parsed = urlparse(s3_prefix)
    s3 = boto3.client(
        "s3",
        aws_access_key_id=env["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=env["AWS_SECRET_ACCESS_KEY"],
        endpoint_url=env.get("S3_ENDPOINT_URL"),
        region_name=env.get("AWS_REGION", "us-east-1")
    )
    response = s3.list_objects_v2(Bucket=parsed.netloc, Prefix=parsed.path.lstrip("/"))
    contents = response.get("Contents", [])
    assert any(obj["Key"].endswith(".parquet") for obj in contents), "No .parquet file found in S3 output"

    print(f"✅ Integration test passed: {len(contents)} files at {s3_prefix}")

@pytest.mark.skipif("POSTGRES_DB_URL" not in env, reason="No Postgres config in .env.test")
def test_postgres_query_export_mode(tmp_path):
    db_url = env["POSTGRES_DB_URL"]
    output_file = tmp_path / "out.parquet"

    result = CliRunner().invoke(run, [
        "--export-mode", "postgres-query",
        "--db-url", db_url,
        "--query", "SELECT 123 AS val",
        "--output-file", str(output_file)
    ])
    assert result.exit_code == 0, f"Postgres export failed: {result.output}"
    assert output_file.exists()

@pytest.mark.skipif("SQLITE_DB_URL" not in env, reason="No SQLite config in .env.test")
def test_sqlite_query_export_mode(tmp_path):
    db_url = env["SQLITE_DB_URL"]
    output_file = tmp_path / "out.parquet"

    result = CliRunner().invoke(run, [
        "--export-mode", "sqlite-query",
        "--db-url", db_url,
        "--query", "SELECT 'abc' AS val",
        "--output-file", str(output_file)
    ])
    assert result.exit_code == 0, f"SQLite export failed: {result.output}"
    assert output_file.exists()
