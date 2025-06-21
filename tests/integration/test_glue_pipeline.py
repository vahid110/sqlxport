# tests/integration/test_glue_pipeline.py

import os
import sys
import uuid
import subprocess
import pytest
from sqlalchemy import text, create_engine

from sqlxport.ddl.utils import generate_athena_ddl
from sqlxport.api.export import run_export, ExportJobConfig, ExportMode
from sqlxport.core.s3_config import S3Config
from sqlxport.cli.glue_ops import (
    register_table_in_glue,
    repair_partitions_in_glue,
    validate_glue_table,
)
from sqlxport.utils.env import load_env_file

# Load environment
os.environ["SQLXPORT_ENV_PATH"] = "tests/.env.test"
env = load_env_file("tests/.env.test")


@pytest.fixture(scope="session", autouse=True)
def seed_logs1_table():
    """Ensure logs1 table exists and is populated."""
    engine = create_engine(env["POSTGRES_DB_URL"])
    with engine.connect() as conn:
        conn.execute(text("""
            DROP TABLE IF EXISTS logs1;
            CREATE TABLE logs1 (
                id SERIAL PRIMARY KEY,
                message TEXT,
                service TEXT
            );
            INSERT INTO logs1 (message, service) VALUES
            ('started service A', 'service_a'),
            ('started service B', 'service_b'),
            ('error in service A', 'service_a');
            GRANT ALL ON logs1 TO testuser;
        """))
        conn.commit()


@pytest.mark.integration
def test_postgres_to_athena_pipeline(tmp_path):
    unique_key = f"test-glue-pipeline/{uuid.uuid4()}/"
    output_dir = "tests/output_partitioned"

    config = ExportJobConfig(
        db_url=env["POSTGRES_DB_URL"],
        query="SELECT * FROM logs1",
        output_dir=output_dir,
        format="parquet",
        partition_by=["service"],
        export_mode=ExportMode("postgres-query"),
        s3_config=S3Config(
            bucket=env["S3_BUCKET"],
            key=unique_key,
            access_key=env["AWS_ACCESS_KEY_ID"],
            secret_key=env["AWS_SECRET_ACCESS_KEY"],
            endpoint_url=env["S3_ENDPOINT_URL"],
            region_name=env["AWS_REGION"]
        ),
        s3_upload=True
    )

    output_path = run_export(config)
    assert os.path.exists(output_path)

    ddl_path = tmp_path / "ddl.sql"
    ddl = generate_athena_ddl(
        local_parquet_path=output_dir,
        s3_prefix=f"s3://{config.s3_config.bucket}/{unique_key}",
        table_name=env["ATHENA_TABLE_NAME"],
        partition_cols=["service"],
        schema_df=[
            {"name": "id", "type": "int64"},
            {"name": "message", "type": "string"},
            {"name": "service", "type": "string"},
        ]
    )
    ddl_path.write_text(ddl)

    register_table_in_glue(
        region=env["AWS_REGION"],
        ddl_path=ddl_path,
        database=env["ATHENA_DATABASE"],
        output=env["ATHENA_OUTPUT_LOCATION"]
    )

    repair_partitions_in_glue(
        region=env["AWS_REGION"],
        table_name=env["ATHENA_TABLE_NAME"],
        database=env["ATHENA_DATABASE"],
        output=env["ATHENA_OUTPUT_LOCATION"]
    )

    validate_glue_table(
        region=env["AWS_REGION"],
        table_name=env["ATHENA_TABLE_NAME"],
        database=env["ATHENA_DATABASE"],
        output=env["ATHENA_OUTPUT_LOCATION"]
    )


@pytest.mark.integration
def test_end_to_end_glue_registration(tmp_path):
    bucket = env["S3_BUCKET"]
    region = env.get("AWS_REGION", "us-east-1")
    db_url = env["POSTGRES_DB_URL"]
    glue_db = env["ATHENA_DATABASE"]
    glue_table = f"logs_test_{uuid.uuid4().hex[:8]}"
    s3_prefix = f"athena-tests/{glue_table}/"
    output_dir = tmp_path / "output"
    partition_by = "service"
    s3_output = f"s3://{bucket}/{s3_prefix}"

    result = subprocess.run([
        sys.executable, "-m", "sqlxport", "export",
        "--db-url", db_url,
        "--export-mode", "postgres-query",
        "--query", "SELECT * FROM logs1",
        "--output-dir", str(output_dir),
        "--partition-by", partition_by,
        "--format", "parquet",
        "--s3-bucket", bucket,
        "--s3-key", s3_prefix,
        "--upload-output-dir",
        "--s3-provider", "aws",
        "--s3-endpoint", env["S3_ENDPOINT_URL"]
    ], capture_output=True, text=True)

    print("🟡 Export stdout:\n", result.stdout)
    print("🔴 Export stderr:\n", result.stderr)
    assert result.returncode == 0, f"Export failed:\n{result.stderr}"

    ddl = generate_athena_ddl(
        local_parquet_path=str(output_dir),
        s3_prefix=s3_output,
        table_name=glue_table,
        partition_cols=[partition_by],
        schema_df=[
            {"name": "id", "type": "int64"},
            {"name": "message", "type": "string"},
            {"name": "service", "type": "string"},
        ]
    )
    ddl_path = tmp_path / "ddl.sql"
    ddl_path.write_text(ddl)

    register_table_in_glue(
        region=region,
        ddl_path=ddl_path,
        database=glue_db,
        output=env["ATHENA_OUTPUT_LOCATION"]
    )

    repair_partitions_in_glue(
        region=region,
        table_name=glue_table,
        database=glue_db,
        output=env["ATHENA_OUTPUT_LOCATION"]
    )

    validate_glue_table(
        region=region,
        table_name=glue_table,
        database=glue_db,
        output=env["ATHENA_OUTPUT_LOCATION"]
    )
