import pytest
import click  # <-- Add this import
from sqlxport.core.export_modes import validate_export_mode

def test_redshift_requires_unload():
    with pytest.raises(click.UsageError, match="not compatible with DB URL"):
        validate_export_mode(export_mode="postgres-query", db_type="redshift://cluster")

def test_postgres_accepts_query():
    assert validate_export_mode(export_mode="postgres-query", db_type="postgresql://host")

def test_sqlite_accepts_query():
    assert validate_export_mode(export_mode="sqlite-query", db_type="sqlite:///file.db")

def test_mysql_accepts_query():
    assert validate_export_mode(export_mode="mysql-query", db_type="mysql://user@host")

def test_invalid_mode():
    with pytest.raises(click.UsageError, match="Unsupported export mode"):
        validate_export_mode(export_mode="foobar", db_type="postgresql://host")
