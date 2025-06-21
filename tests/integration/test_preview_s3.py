import pytest
from click.testing import CliRunner
from sqlxport.cli.cmd_preview import preview
import os


@pytest.mark.integration
def test_list_s3_objects(monkeypatch):
    runner = CliRunner()

    # Use test credentials (assuming MinIO or S3 mock is running)
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "minioadmin")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "minioadmin")
    monkeypatch.setenv("AWS_REGION", "us-east-1")

    result = runner.invoke(preview, [
        "--list-s3",
        "--bucket", "vahid-signing",
        "--key", "test-data/",
        "--access-key", "minioadmin",
        "--secret-key", "minioadmin",
        "--endpoint-url", "http://localhost:9000"
    ])
    assert result.exit_code == 0
    assert "Found" in result.output or "s3://" in result.output


@pytest.mark.integration
def test_preview_s3_parquet(monkeypatch):
    runner = CliRunner()

    # Assume "test-data/sample.parquet" exists in MinIO/S3 bucket
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "minioadmin")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "minioadmin")
    monkeypatch.setenv("AWS_REGION", "us-east-1")

    result = runner.invoke(preview, [
        "--s3-file",
        "--bucket", "vahid-signing",
        "--key", "test-data/sample.parquet",
        "--access-key", "minioadmin",
        "--secret-key", "minioadmin",
        "--endpoint-url", "http://localhost:9000"
    ])
    assert result.exit_code == 0
    assert "Previewing S3 Parquet file" in result.output or "rows" in result.output

