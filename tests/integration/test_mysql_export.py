import os
import sys
import subprocess
import pytest
from sqlalchemy import create_engine, text

env = {
    "MYSQL_DB_URL": "mysql+pymysql://testuser:testpass@localhost:3307/testdb"
}


def wait_for_mysql():
    import time
    import pymysql
    for _ in range(30):
        try:
            conn = pymysql.connect(
                host="localhost", port=3307, user="testuser", password="testpass", database="testdb"
            )
            conn.close()
            return
        except Exception:
            time.sleep(1)
    raise Exception("MySQL did not become ready in time")


def setup_mysql_data():
    engine = create_engine(env["MYSQL_DB_URL"])
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS users (
                id INT PRIMARY KEY AUTO_INCREMENT,
                name VARCHAR(255),
                age INT
            )
        """))
        conn.execute(text("DELETE FROM users"))  # Clean slate
        conn.execute(text("""
            INSERT INTO users (name, age) VALUES
            ('Alice', 30),
            ('Bob', 25),
            ('Charlie', 35)
        """))


@pytest.mark.integration
def test_mysql_query_to_parquet(tmp_path):
    wait_for_mysql()
    setup_mysql_data()

    output_file = tmp_path / "output.parquet"
    result = subprocess.run([
        sys.executable, "-m", "sqlxport", "export",
        "--db-url", env["MYSQL_DB_URL"],
        "--export-mode", "mysql-query",
        "--query", "SELECT * FROM users",
        "--output-file", str(output_file),
        "--format", "parquet"
    ], capture_output=True, text=True)

    print("STDOUT:\n", result.stdout)
    print("STDERR:\n", result.stderr)
    assert result.returncode == 0, f"Export failed: {result.stderr}"
    assert output_file.exists()
