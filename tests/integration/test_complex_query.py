# tests/integration/test_complex_query.py

import os
import subprocess
from pathlib import Path

import boto3
import duckdb
import pytest
from dotenv import load_dotenv


@pytest.mark.integration
def test_postgres_complex_query(tmp_path):
    output_dir = tmp_path / "output_complex_postgres"
    load_dotenv(dotenv_path="tests/.env.test")

    db_url = os.environ["POSTGRES_DB_URL"]

    complex_query = """
        WITH customer_totals AS (
            SELECT
                c.name,
                c.country,
                COUNT(o.id) AS order_count,
                SUM(o.total_amount) AS total_spent
            FROM customers c
            JOIN orders o ON c.id = o.customer_id
            WHERE c.country = 'Germany'
            GROUP BY c.name, c.country
        )
        SELECT * FROM customer_totals
        ORDER BY total_spent DESC
    """

    result = subprocess.run([
        "sqlxport", "export",
        "--db-url", db_url,
        "--query", complex_query,
        "--output-dir", str(output_dir),
        "--format", "parquet",
        "--export-mode", "postgres-query"
    ], capture_output=True, text=True)

    assert result.returncode == 0, f"Export failed:\n{result.stderr}"
    assert output_dir.exists(), "Output directory was not created"
    assert any(f.name.endswith(".parquet") for f in output_dir.iterdir()), "No Parquet file generated"


@pytest.mark.integration
def test_upload_and_validate_athena(tmp_path):
    load_dotenv(dotenv_path="tests/.env.test")

    db_url = os.environ["POSTGRES_DB_URL"]
    output_dir = tmp_path / "output_complex_postgres"
    bucket = os.environ["S3_BUCKET"]
    s3_prefix = os.path.join(os.environ["ATHENA_S3_PREFIX_PARQUET"], "complex_query")
    region = os.environ["AWS_REGION"]
    output_location = os.environ["ATHENA_OUTPUT_LOCATION"]

    query = """
        WITH customer_totals AS (
            SELECT
                c.name,
                c.country,
                COUNT(o.id) AS order_count,
                SUM(o.total_amount) AS total_spent
            FROM customers c
            JOIN orders o ON c.id = o.customer_id
            WHERE c.country = 'Germany'
            GROUP BY c.name, c.country
        )
        SELECT * FROM customer_totals
        ORDER BY total_spent DESC
    """

    # Export query result using sqlxport
    subprocess.run([
        "sqlxport", "export",
        "--db-url", db_url,
        "--query", query,
        "--output-dir", str(output_dir),
        "--format", "parquet",
        "--export-mode", "postgres-query"
    ], check=True)

    # Upload Parquet files to S3
    s3 = boto3.client("s3", region_name=region)
    try:
        s3.head_bucket(Bucket=bucket)
    except s3.exceptions.ClientError:
        s3.create_bucket(Bucket=bucket)

    for file in Path(output_dir).rglob("*.parquet"):
        s3_key = f"{s3_prefix}/{file.name}"
        s3.upload_file(str(file), bucket, s3_key)

    print(f"✅ Uploaded to s3://{bucket}/{s3_prefix}/")

    s3_path = f"s3://{bucket}/{s3_prefix}"

    # Run Athena preview using sqlxport
    preview_result = subprocess.run([
        "sqlxport", "preview",
        "--file-query-engine", "athena",
        "--local-dir", str(output_dir),  # for schema inference
        "--engine-args", f"region={region}",
        "--engine-args", f"database=default",
        "--engine-args", f"output_location={output_location}",
        "--engine-args", f"s3_path={s3_path}",
        "--engine-args", "partition_cols=name,country"
    ], capture_output=True, text=True)

    assert preview_result.returncode == 0, f"Athena validation failed:\n{preview_result.stderr}"
    assert "total_spent" in preview_result.stdout.lower(), "Athena result does not contain expected column"



@pytest.mark.integration
def test_duckdb_validate_complex_query(tmp_path):
    load_dotenv(dotenv_path="tests/.env.test")

    db_url = os.environ["POSTGRES_DB_URL"]
    output_dir = tmp_path / "output_duckdb_postgres"
    output_dir.mkdir(parents=True, exist_ok=True)

    complex_query = """
        WITH customer_totals AS (
            SELECT
                c.name,
                c.country,
                COUNT(o.id) AS order_count,
                SUM(o.total_amount) AS total_spent
            FROM customers c
            JOIN orders o ON c.id = o.customer_id
            WHERE c.country = 'Germany'
            GROUP BY c.name, c.country
        )
        SELECT * FROM customer_totals
        ORDER BY total_spent DESC
    """

    result = subprocess.run([
        "sqlxport", "export",
        "--db-url", db_url,
        "--query", complex_query,
        "--output-dir", str(output_dir),
        "--format", "parquet",
        "--export-mode", "postgres-query"
    ], capture_output=True, text=True)

    assert result.returncode == 0, f"Export failed:\n{result.stderr}"

    parquet_files = list(Path(output_dir).rglob("*.parquet"))
    assert parquet_files, "No Parquet file found in output directory."

    df = duckdb.read_parquet(str(parquet_files[0])).to_df()
    assert not df.empty, "Exported Parquet file is empty."

    row_count = duckdb.query(f"SELECT COUNT(*) FROM read_parquet('{parquet_files[0]}')").fetchone()[0]
    assert row_count > 0, "DuckDB validation failed: no rows in exported data."
