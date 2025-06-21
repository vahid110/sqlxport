# tests/unit/test_preview_schema_error.py

import pytest
from click.testing import CliRunner
from sqlxport.cli.cmd_preview import preview_schema

@pytest.mark.unit
def test_preview_schema_with_malformed_sql():
    db_url = "sqlite:///:memory:"
    malformed_query = "SELECT FROM WHERE"

    result = CliRunner().invoke(preview_schema, [
        "--db-url", db_url,
        "--query", malformed_query
    ])

    assert result.exit_code != 0
    assert "syntax error" in result.output.lower() or "error" in result.output.lower()