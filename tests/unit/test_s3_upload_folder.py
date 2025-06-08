# tests/unit/test_s3_upload_folder.py
import os
import tempfile
import unittest
from unittest.mock import patch, MagicMock
from sqlxport.core.storage import upload_folder_to_s3

class TestS3FolderUpload(unittest.TestCase):

    @patch("boto3.session.Session")
    def test_upload_folder_to_s3(self, mock_session_cls):
        # Mock S3 client
        mock_session = MagicMock()
        mock_s3 = MagicMock()
        mock_session.client.return_value = mock_s3
        mock_session_cls.return_value = mock_session

        # Create temporary folder with fake files
        with tempfile.TemporaryDirectory() as tmpdir:
            f1 = os.path.join(tmpdir, "file1.txt")
            f2 = os.path.join(tmpdir, "nested", "file2.txt")
            os.makedirs(os.path.dirname(f2), exist_ok=True)
            open(f1, "w").write("Hello")
            open(f2, "w").write("World")

            # Run function
            upload_folder_to_s3(
                local_dir=tmpdir,
                bucket="my-bucket",
                prefix="test",
                access_key="KEY",
                secret_key="SECRET",
                endpoint_url="https://s3.amazonaws.com"
            )

            # Expect 2 uploads
            calls = [call[0] for call in mock_s3.upload_file.call_args_list]
            uploaded_files = [os.path.basename(args[0]) for args in calls]
            assert "file1.txt" in uploaded_files
            assert "file2.txt" in uploaded_files
