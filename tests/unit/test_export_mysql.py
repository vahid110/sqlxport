# tests/integration/test_mysql_export.py
import os
import uuid
import subprocess
import pytest
import sys

from sqlxport.utils.env import load_env_file

# Load test environment
os.environ["SQLXPORT_ENV_PATH"] = "tests/.env.test"
env = load_env_file("tests/.env.test")

@pytest.mark.integration
def test_mysql_query_to_parquet(tmp_path):
    table = "users"
    output_file = tmp_path / "output.parquet"

    result = subprocess.run([
        sys.executable, "-m", "sqlxport", "export",
        "--db-url", env["MYSQL_DB_URL"],
        "--export-mode", "mysql-query",
        "--query", f"SELECT * FROM {table}",
        "--output-file", str(output_file),
        "--format", "parquet"
    ], capture_output=True, text=True)

    print("STDOUT:\n", result.stdout)
    print("STDERR:\n", result.stderr)
    assert result.returncode == 0, f"Export failed: {result.stderr}"
    assert output_file.exists()
