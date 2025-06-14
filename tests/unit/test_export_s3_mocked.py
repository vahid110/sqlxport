import pandas as pd
import tempfile
import os
from unittest.mock import patch
from sqlxport.api.export import ExportJobConfig, ExportMode, run_export
from sqlxport.core.s3_config import S3Config

def mock_fetch(*_):
    return pd.DataFrame({
        "x": [1, 2, 3]
    })

@patch("sqlxport.api.export.upload_file_to_s3")
def test_file_upload_mocked(mock_upload):
    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmpfile:
        output_file = tmpfile.name

    config = ExportJobConfig(
        db_url="sqlite://",
        query="SELECT * FROM dummy",
        export_mode=ExportMode("sqlite-query"),
        format="parquet",
        output_file=output_file,
        s3_upload=True,
        s3_config=S3Config(bucket="mybucket", key="mykey")
    )

    run_export(config, fetch_override=mock_fetch)
    mock_upload.assert_called_once()
    os.remove(output_file)

@patch("sqlxport.api.export.upload_dir_to_s3")
def test_dir_upload_mocked(mock_upload, tmp_path):
    output_dir = tmp_path / "data"
    config = ExportJobConfig(
        db_url="sqlite://",
        query="SELECT * FROM dummy",
        export_mode=ExportMode("sqlite-query"),
        format="parquet",
        output_dir=output_dir,
        partition_by=["x"],
        s3_upload=True,
        s3_config=S3Config(bucket="mybucket", key="mykey")
    )

    run_export(config, fetch_override=mock_fetch)
    mock_upload.assert_called_once()
