# tests/unit/test_redshift_unload.py
import unittest
from unittest.mock import patch, MagicMock
from sqlxport.redshift_unload import run_unload

class TestRedshiftUnload(unittest.TestCase):

    @patch("psycopg.connect")  # ✅ Patch psycopg globally
    def test_run_unload_executes_sql(self, mock_connect):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_connect.return_value.__enter__.return_value = mock_conn
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

        run_unload(
            db_url="postgresql://test",
            query="SELECT * FROM users",
            s3_output_prefix="s3://bucket/prefix/",
            iam_role="arn:aws:iam::123456789012:role/test-role"
        )

        assert mock_cursor.execute.called
        executed_sql = mock_cursor.execute.call_args[0][0]
        assert "UNLOAD" in executed_sql
        assert "IAM_ROLE" in executed_sql
        assert "SELECT * FROM users" in executed_sql

