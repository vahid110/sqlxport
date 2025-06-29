import os
import pytest
import subprocess
from pathlib import Path
from dotenv import load_dotenv, dotenv_values
import boto3

from pathlib import PurePosixPath


# Load credentials and settings from the test .env
dotenv_path=Path(__file__).parent.parent / ".env.test"
load_dotenv(dotenv_path=dotenv_path)
config = dotenv_values(dotenv_path)


def download_from_s3_to_local_path(config, s3_key):
    import tempfile
    import shutil

    local_output_dir = tempfile.mkdtemp(prefix="sqlxport_")

    # Download files from S3
    s3 = boto3.client(
        "s3",
        endpoint_url=config["S3_ENDPOINT_URL"],
        aws_access_key_id=config["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=config["AWS_SECRET_ACCESS_KEY"],
    )

    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=config["S3_BUCKET"], Prefix=s3_key):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.endswith(".parquet"):
                continue
            local_path = os.path.join(local_output_dir, os.path.basename(key))
            s3.download_file(config["S3_BUCKET"], key, local_path)
    return local_output_dir


def cleanup_s3_prefix(bucket, prefix, endpoint_url, aws_access_key_id, aws_secret_access_key):
    s3 = boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
    )

    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        if "Contents" in page:
            objects = [{"Key": obj["Key"]} for obj in page["Contents"]]
            s3.delete_objects(Bucket=bucket, Delete={"Objects": objects})



@pytest.mark.integration
def test_redshift_to_athena_end_to_end():
    """
    Full integration test: Redshift → S3 → Glue → Athena
    - Runs sqlxport to export from Redshift to S3
    - Generates Glue-compatible DDL
    - Registers table
    - Runs validation query in Athena
    """
    env = os.environ.copy()
    output_dir = "test_output_redshift"

    s3_key = str(PurePosixPath(env["ATHENA_S3_PREFIX_PARQUET"]) / output_dir) + "/"

    # Clean up S3 prefix before export
    cleanup_s3_prefix(
        bucket=env["S3_BUCKET"],
        prefix=s3_key,
        endpoint_url=env["S3_ENDPOINT_URL"],
        aws_access_key_id=config["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=config["AWS_SECRET_ACCESS_KEY"],
    )

    # print(env)
    # Step 1: Export Parquet using Redshift UNLOAD
    export_cmd = [
        "sqlxport", "export",
        "--db-url", env["REDSHIFT_DB_URL"],
        "--export-mode", "redshift-unload",
        "--query", "SELECT * FROM users",
        # "--output-dir", output_dir,
        "--format", "parquet",
        "--s3-output-prefix", f"s3://{env['S3_BUCKET']}/{s3_key}",
        "--s3-bucket", env["S3_BUCKET"],
        "--s3-key", s3_key,
        "--s3-access-key", config["AWS_ACCESS_KEY_ID"],
        "--s3-secret-key", config["AWS_SECRET_ACCESS_KEY"],
        "--iam-role", env["REDSHIFT_IAM_ROLE"],
        "--s3-endpoint", env["S3_ENDPOINT_URL"],
        "--upload-output-dir"
    ]

    # print(export_cmd)
    result = subprocess.run(export_cmd, capture_output=True, text=True, env=env)
    assert result.returncode == 0, f"Export failed:\n{result.stderr}"

    local_output_dir = download_from_s3_to_local_path(config, s3_key)
    # Step 2: Generate Glue DDL
    ddl_cmd = [
        "sqlxport", "generate-ddl",
        "--input-file", local_output_dir,
        "--output-format", "athena",
        "--table-name", "test_users_redshift",
        "--s3-url", f"s3://{env['S3_BUCKET']}/{s3_key}"
    ]

    result = subprocess.run(ddl_cmd, capture_output=True, text=True, env=env)
    assert result.returncode == 0, f"DDL generation failed:\n{result.stderr}"

    # ✅ Save DDL output manually
    ddl_file = Path("test_glue_table.sql")
    ddl_file.write_text(result.stdout)
    assert ddl_file.exists()


    # Step 3: Register Glue table
    glue_cmd = [
        "aws", "athena", "start-query-execution",
        "--query-string", Path("test_glue_table.sql").read_text(),
        "--query-execution-context", f"Database={env['ATHENA_DB_NAME']}",
        "--result-configuration", f"OutputLocation={env['ATHENA_OUTPUT_LOCATION']}"
    ]
    result = subprocess.run(glue_cmd, capture_output=True, text=True, env=env)
    assert result.returncode == 0, f"Glue registration failed:\n{result.stderr}"

    # Step 4: Validate Athena query
    validate_cmd = [
        "sqlxport", "preview",
        "--file-query-engine", "athena",
        "--s3-file",
        "--bucket", env["S3_BUCKET"],
        "--key", s3_key + "0000_part_00.parquet",
        "--access-key", config["AWS_ACCESS_KEY_ID"],
        "--secret-key", config["AWS_SECRET_ACCESS_KEY"],
        "--endpoint-url", env["S3_ENDPOINT_URL"],
        "--engine-args", f"database={env['ATHENA_DB_NAME']}",
        "--engine-args", f"output={env['ATHENA_OUTPUT_LOCATION']}",
        "--engine-args", f"region={env['AWS_REGION']}"
    ]


    result = subprocess.run(validate_cmd, capture_output=True, text=True, env=env)
    assert result.returncode == 0, f"Athena validation failed:\n{result.stderr}"
    assert any(col in result.stdout.lower() for col in ["userid", "user_id", "user id"])

