# tests/unit/test_s3_upload.py
import unittest
from unittest.mock import patch, MagicMock
from sqlxport.core.storage import upload_file_to_s3

class TestS3Upload(unittest.TestCase):

    @patch("boto3.client")
    def test_upload_file_to_s3(self, mock_boto_client):
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3

        upload_file_to_s3(
            file_path="/tmp/example.parquet",
            bucket_name="my-bucket",
            object_key="test/example.parquet",
            access_key="AKIA_TEST",
            secret_key="SECRET_TEST",
            endpoint_url="https://s3.amazonaws.com"
        )

        mock_boto_client.assert_called_once()
        mock_s3.upload_file.assert_called_once_with(
            "/tmp/example.parquet", "my-bucket", "test/example.parquet"
        )
