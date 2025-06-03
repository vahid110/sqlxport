# tests/unit/test_storage.py

import os
import unittest
import tempfile
from unittest.mock import patch, MagicMock
from sqlxport.core.storage import upload_file_to_s3
from sqlxport.core.storage import upload_folder_to_s3


@patch("sqlxport.core.storage.boto3.client")
def test_upload_file_to_s3(mock_boto_client):
    # Create a temporary test file
    with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
        tmp_file.write(b"Test content")
        tmp_file_path = tmp_file.name

    mock_s3 = MagicMock()
    mock_boto_client.return_value = mock_s3

    # Call function under test
    upload_file_to_s3(
        file_path=tmp_file_path,
        bucket_name="test-bucket",
        object_key="test/key.parquet",
        access_key="AKIA_TEST",
        secret_key="SECRET_TEST",
        endpoint_url="http://localhost:9000",
        region_name="us-east-1"
    )

    # Assert boto3 client was created with correct parameters
    mock_boto_client.assert_called_with(
        's3',
        aws_access_key_id="AKIA_TEST",
        aws_secret_access_key="SECRET_TEST",
        endpoint_url="http://localhost:9000",
        region_name="us-east-1"
    )

    # Assert upload_file was called
    mock_s3.upload_file.assert_called_once_with(
        tmp_file_path, "test-bucket", "test/key.parquet"
    )

    os.unlink(tmp_file_path)  # cleanup

class TestStorageFailures(unittest.TestCase):

    @patch("sqlxport.core.storage.boto3")
    def test_upload_with_missing_file(self, mock_boto3):
        mock_client = MagicMock()
        mock_client.upload_file.side_effect = FileNotFoundError("File not found")
        mock_boto3.client.return_value = mock_client

        with self.assertRaises(FileNotFoundError):
            upload_file_to_s3(
                file_path="nonexistent.parquet",
                bucket_name="test-bucket",
                object_key="key",
                access_key="a",
                secret_key="s",
                endpoint_url="http://localhost"
            )

class TestStorageErrorCases(unittest.TestCase):

    @patch("sqlxport.core.storage.boto3")
    def test_upload_with_missing_file(self, mock_boto3):
        mock_client = MagicMock()
        mock_client.upload_file.side_effect = FileNotFoundError("File not found")
        mock_boto3.client.return_value = mock_client

        with self.assertRaises(FileNotFoundError):
            upload_file_to_s3(
                file_path="nonexistent.parquet",
                bucket_name="test-bucket",
                object_key="key",
                access_key="a",
                secret_key="s",
                endpoint_url="http://localhost"
            )

class TestUploadFolder(unittest.TestCase):
    @patch("sqlxport.core.storage.boto3.session.Session")
    @patch("sqlxport.core.storage.os.walk")
    def test_upload_folder_to_s3(self, mock_walk, mock_session):
        # Simulate two files
        mock_walk.return_value = [
            ("/fake/dir", ["subdir"], ["file1.parquet", "file2.parquet"])
        ]

        mock_s3 = MagicMock()
        mock_session.return_value.client.return_value = mock_s3

        upload_folder_to_s3(
            local_dir="/fake/dir",
            bucket="my-bucket",
            prefix="exports",
            access_key="abc",
            secret_key="xyz",
            endpoint_url="http://localhost:9000"
        )

        # Should upload both files
        calls = [
            ("/fake/dir/file1.parquet", "my-bucket", "exports/file1.parquet"),
            ("/fake/dir/file2.parquet", "my-bucket", "exports/file2.parquet")
        ]
        actual_calls = [c.args for c in mock_s3.upload_file.call_args_list]
        assert actual_calls == calls
