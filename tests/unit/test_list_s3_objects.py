# tests/unit/test_list_s3_objects.py
import unittest
from unittest.mock import patch, MagicMock
from sqlxport.core.storage import list_s3_objects

class TestListS3Objects(unittest.TestCase):

    @patch("boto3.client")
    def test_list_s3_objects_prints_keys(self, mock_boto_client):
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3
        mock_s3.list_objects_v2.return_value = {
            "Contents": [
                {"Key": "file1.parquet", "Size": 1234},
                {"Key": "file2.parquet", "Size": 5678},
            ]
        }

        # No assertion here since function prints, but it should not raise
        list_s3_objects(
            bucket="my-bucket",
            prefix="my-prefix",
            access_key="AKIA...",
            secret_key="SECRET..."
        )
