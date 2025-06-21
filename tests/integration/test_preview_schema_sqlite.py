# tests/integration/test_preview_schema_sqlite.py

import sqlite3
import pytest
from pathlib import Path
from click.testing import CliRunner
from sqlxport.cli.cmd_preview import preview_schema
from sqlalchemy import create_engine, text

@pytest.mark.integration
def test_preview_schema_sqlite_basic(tmp_path):
    db_path = tmp_path / "schema_test.sqlite"
    db_url = f"sqlite:///{db_path}"

    engine = create_engine(db_url)
    with engine.connect() as conn:
        conn.execute(text("CREATE TABLE test_data (id INTEGER, name TEXT, active BOOLEAN)"))

    result = CliRunner().invoke(preview_schema, [
        "--db-url", db_url,
        "--query", "SELECT * FROM test_data"
    ])

    assert result.exit_code == 0
    assert "id" in result.output
    assert "name" in result.output
    assert "active" in result.output

@pytest.mark.integration
def test_preview_schema_sqlite_json(tmp_path):
    db_path = tmp_path / "test.sqlite"
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute("CREATE TABLE test (id INTEGER, name TEXT)")
    cur.executemany("INSERT INTO test VALUES (?, ?)", [(1, 'Alice'), (2, 'Bob')])
    conn.commit()
    conn.close()

    db_url = f"sqlite:///{db_path}"
    result = CliRunner().invoke(preview_schema, [
        "--db-url", db_url,
        "--query", "SELECT * FROM test",
        "--as-json"
    ])

    assert result.exit_code == 0
    assert '"name": "id"' in result.output
    assert '"name": "name"' in result.output