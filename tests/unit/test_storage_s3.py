# tests/unit/test_storage_s3.py

import unittest
from unittest.mock import patch, MagicMock
from sqlxport.core.storage import list_s3_objects, preview_s3_parquet

class TestS3Helpers(unittest.TestCase):

    @patch("sqlxport.core.storage.boto3.client")
    def test_list_s3_objects_empty(self, mock_client):
        mock_s3 = MagicMock()
        mock_s3.list_objects_v2.return_value = {}
        mock_client.return_value = mock_s3

        list_s3_objects(
            bucket="test-bucket",
            prefix="some/prefix",
            access_key="key",
            secret_key="secret"
        )

        mock_s3.list_objects_v2.assert_called_once()

    @patch("sqlxport.core.storage.boto3.client")
    def test_preview_s3_parquet_success(self, mock_client):
        import pyarrow as pa
        import pyarrow.parquet as pq
        import io
        import pandas as pd

        table = pa.table({"col1": pa.array([1, 2])})
        buf = io.BytesIO()
        pq.write_table(table, buf)
        buf.seek(0)

        mock_s3 = MagicMock()
        mock_s3.get_object.return_value = {"Body": buf}
        mock_client.return_value = mock_s3

        preview_s3_parquet(
            bucket="bucket",
            key="key.parquet",
            access_key="a",
            secret_key="b"
        )
