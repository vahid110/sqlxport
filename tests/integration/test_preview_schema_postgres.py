# tests/integration/test_preview_schema_postgres.py

import pytest
import sqlalchemy
from click.testing import CliRunner
from sqlxport.cli.cmd_preview import preview_schema
from sqlxport.utils.env import load_env_file

env = load_env_file("tests/.env.test")

@pytest.mark.integration
@pytest.mark.skipif("POSTGRES_DB_URL" not in env, reason="No Postgres config in .env.test")
def test_preview_schema_postgres():
    db_url = env["POSTGRES_DB_URL"]

    engine = sqlalchemy.create_engine(db_url)
    with engine.begin() as conn:
        conn.execute(sqlalchemy.text("DROP TABLE IF EXISTS preview_test"))
        conn.execute(sqlalchemy.text("CREATE TABLE preview_test (id INTEGER, name TEXT, active BOOLEAN)"))
        conn.execute(sqlalchemy.text("INSERT INTO preview_test VALUES (1, 'Alice', true), (2, 'Bob', false)"))

    result = CliRunner().invoke(preview_schema, [
        "--db-url", db_url,
        "--query", "SELECT * FROM preview_test",
        "--as-json"
    ])

    assert result.exit_code == 0, result.output
    assert '"name": "id"' in result.output
    assert '"name": "name"' in result.output
    assert '"name": "active"' in result.output


@pytest.mark.integration
def test_preview_schema_postgres2(tmp_path):

    db_url = env.get("POSTGRES_DB_URL")
    if not db_url:
        pytest.skip("POSTGRES_DB_URL not configured in .env.test")

    engine = sqlalchemy.create_engine(db_url)
    with engine.begin() as conn:
        conn.execute(sqlalchemy.text("DROP TABLE IF EXISTS preview_test"))
        conn.execute(sqlalchemy.text("CREATE TABLE preview_test (id SERIAL PRIMARY KEY, data TEXT)"))
        conn.execute(sqlalchemy.text("INSERT INTO preview_test (data) VALUES ('abc'), ('xyz')"))

    result = CliRunner().invoke(preview_schema, [
        "--db-url", db_url,
        "--query", "SELECT * FROM preview_test",
        "--as-json"
    ])

    assert result.exit_code == 0, result.output
    assert '"name": "id"' in result.output
    assert '"name": "data"' in result.output