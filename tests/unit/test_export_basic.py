import os
import pandas as pd
import pytest
from sqlxport.api.export import ExportJobConfig, ExportMode, run_export, S3Config

def mock_fetch(_db_url, _query):
    return pd.DataFrame({
        "id": [1, 2],
        "group": ["A", "B"]
    })

def test_export_query_file_output(tmp_path):
    config = ExportJobConfig(
        db_url="sqlite://",
        query="SELECT 1",
        output_file=str(tmp_path / "output.parquet"),
        format="parquet",
        export_mode=ExportMode("sqlite-query")
    )
    path = run_export(config, fetch_override=mock_fetch)
    assert os.path.exists(path)
    assert path.endswith(".parquet")

def test_export_query_dir_partitioned(tmp_path):
    config = ExportJobConfig(
        db_url="sqlite://",
        query="SELECT 1",
        output_dir=str(tmp_path),
        format="parquet",
        partition_by=["group"],
        export_mode=ExportMode("sqlite-query")
    )
    output_path = run_export(config, fetch_override=mock_fetch)
    assert os.path.isdir(output_path)
    assert any("group=A" in str(p) for p in os.listdir(output_path))

def test_export_upload_file_to_s3(tmp_path, monkeypatch):
    uploaded = {}

    monkeypatch.setattr(
        "sqlxport.api.export.upload_file_to_s3",
        lambda file_path, bucket_name, object_key, **kwargs: uploaded.update({
            "file_path": file_path,
            "bucket": bucket_name,
            "key": object_key
        })
    )

    config = ExportJobConfig(
        db_url="sqlite://",
        query="SELECT 1",
        output_file=str(tmp_path / "upload.parquet"),
        format="parquet",
        export_mode=ExportMode("sqlite-query"),
        s3_config=S3Config(
            bucket="my-bucket",
            key="path/to/file",
            access_key="x",
            secret_key="y",
            endpoint_url="http://localhost:9000",
            region_name="us-east-1"
        ),
        s3_upload=True
    )

    path = run_export(config, fetch_override=mock_fetch)
    assert uploaded["file_path"] == path
    assert uploaded["bucket"] == "my-bucket"
    assert uploaded["key"] == "path/to/file"

