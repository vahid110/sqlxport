#tests/integration/test_csv_preview.py

import os
import subprocess
import pytest


@pytest.fixture(scope="module")
def csv_output_dir(tmp_path_factory):
    return tmp_path_factory.mktemp("csv_output")


from dotenv import load_dotenv

def test_csv_export_and_preview(csv_output_dir):
    env_path = os.path.join(os.path.dirname(__file__), "../.env.test")
    load_dotenv(env_path)  # 👈 load env vars into current Python process
    db_url = os.getenv("DB_URL")
    assert db_url is not None, "❌ DB_URL not set"

    print(f'DB_URL={db_url}')
    result = subprocess.run([
        "sqlxport", "export",
        "--env-file", env_path,
        "--query", "SELECT 'hello' AS greeting, 42 AS number",
        "--output-dir", str(csv_output_dir),
        "--format", "csv",
        "--export-mode", "postgres-query"
    ], capture_output=True, text=True)


    print("EXPORT STDOUT:", result.stdout)
    print("EXPORT STDERR:", result.stderr)
    assert result.returncode == 0

    result = subprocess.run([
        "sqlxport", "preview",
        "--local-file", str(csv_output_dir / "output.csv"),
        "--file-query-engine", "duckdb",
        "--engine-args", "header=True"
    ], capture_output=True, text=True)



    print("PREVIEW STDOUT:", result.stdout)
    print("PREVIEW STDERR:", result.stderr)
    assert result.returncode == 0
    assert "greeting" in result.stdout
    assert "hello" in result.stdout

