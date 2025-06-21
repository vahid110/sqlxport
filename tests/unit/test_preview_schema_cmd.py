# tests/unit/test_preview_schema_cmd.py

import pytest
from click.testing import CliRunner
from sqlalchemy import text
from sqlxport.cli.cmd_preview import preview_schema

@pytest.mark.unit
def test_preview_schema_sqlite_json(tmp_path):
    from sqlalchemy import create_engine
    db_path = tmp_path / "test.sqlite"
    db_url = f"sqlite:///{db_path}"
    engine = create_engine(db_url)

    with engine.connect() as conn:
        conn.execute(text("CREATE TABLE people (id INTEGER, name TEXT)"))
        conn.execute(text("INSERT INTO people VALUES (1, 'Alice'), (2, 'Bob')"))

    runner = CliRunner()
    result = runner.invoke(preview_schema, [
        "--db-url", db_url,
        "--query", "SELECT * FROM people",
        "--as-json"
    ])

    assert result.exit_code == 0
    assert '"name": "id"' in result.output
    assert '"name": "name"' in result.output

@pytest.mark.unit
def test_preview_schema_sqlite_plain(tmp_path):
    from sqlalchemy import create_engine
    db_path = tmp_path / "plain.sqlite"
    db_url = f"sqlite:///{db_path}"
    engine = create_engine(db_url)

    with engine.connect() as conn:
        conn.execute(text("CREATE TABLE items (sku TEXT, price REAL)"))

    runner = CliRunner()
    result = runner.invoke(preview_schema, [
        "--db-url", db_url,
        "--query", "SELECT * FROM items"
    ])

    assert result.exit_code == 0
    assert "📄 Schema Preview" in result.output
    assert "sku" in result.output
    assert "price" in result.output

@pytest.mark.unit
def test_preview_schema_missing_args():
    runner = CliRunner()
    result = runner.invoke(preview_schema, [])
    assert result.exit_code != 0
    assert "Missing option '--db-url'" in result.output

@pytest.mark.unit
def test_preview_schema_output_formats(monkeypatch):
    import pandas as pd

    class DummyConn:
        def __enter__(self): return self
        def __exit__(self, *a): pass

    class DummyEngine:
        def connect(self): return DummyConn()

    monkeypatch.setattr("sqlxport.cli.cmd_preview.create_engine", lambda _: DummyEngine())
    monkeypatch.setattr("sqlxport.cli.cmd_preview.pd.read_sql_query", lambda q, conn: pd.DataFrame({"id": [1], "name": ["A"]}))

    runner = CliRunner()
    result = runner.invoke(preview_schema, [
        "--db-url", "sqlite://",
        "--query", "SELECT * FROM dummy",
        "--as-json"
    ])
    assert result.exit_code == 0
    assert '"name": "id"' in result.output

    result = runner.invoke(preview_schema, [
        "--db-url", "sqlite://",
        "--query", "SELECT * FROM dummy",
        "--as-yaml"
    ])
    assert result.exit_code == 0
    assert "- name: id" in result.output

    result = runner.invoke(preview_schema, [
        "--db-url", "sqlite://",
        "--query", "SELECT * FROM dummy"
    ])
    assert result.exit_code == 0
    assert "📄 Schema Preview" in result.output