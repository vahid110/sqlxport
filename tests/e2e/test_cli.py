import os
import pandas as pd
import pytest
from click.testing import CliRunner
from sqlalchemy import create_engine, text
from sqlxport.cli.main import cli
from sqlxport.api.export import ExportJobConfig, ExportMode, run_export

def create_sample_sqlite_db():
    engine = create_engine("sqlite:///test.db")
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE IF NOT EXISTS users (id INTEGER, name TEXT)"))
        conn.execute(text("INSERT INTO users (id, name) VALUES (1, 'Alice'), (2, 'Bob')"))

def test_cli_output_file(tmp_path, monkeypatch):
    monkeypatch.setattr("sqlxport.api.export.upload_file_to_s3", lambda *a, **kw: None)
    monkeypatch.setattr("sqlxport.api.export.fetch_query_as_dataframe", lambda *_: pd.DataFrame({"id": [1], "name": ["Alice"]}))

    runner = CliRunner()
    output_file = tmp_path / "out.parquet"
    result = runner.invoke(cli, [
        "export",
        "--query", "SELECT id, name FROM users",
        "--output-file", str(output_file),
        "--format", "parquet",
        "--export-mode", "sqlite-query",
        "--db-url", "sqlite:///test.db"
    ])
    assert result.exit_code == 0
    assert output_file.exists()

def test_redshift_unload_requires_iam_role():
    runner = CliRunner()
    result = runner.invoke(cli, [
        "export",
        "--db-url", "redshift://example.us-east-1.redshift.amazonaws.com:5439/dev",
        "--query", "SELECT 1",
        "--export-mode", "redshift-unload",
        "--s3-output-prefix", "s3://dummy/prefix/"
    ])
    assert result.exit_code == 2
    assert "requires --redshift-unload-role" in result.output

def test_preview_local_file_invalid_path():
    runner = CliRunner()
    result = runner.invoke(cli, [
        "preview",
        "--local-file", "nonexistent.parquet"
    ])
    assert result.exit_code != 0
    assert result.exception is not None
    assert "no files found" in str(result.exception).lower()

def test_cli_output_csv_file(tmp_path, monkeypatch):
    monkeypatch.setattr("sqlxport.api.export.upload_file_to_s3", lambda *a, **kw: None)
    monkeypatch.setattr("sqlxport.api.export.fetch_query_as_dataframe", lambda *_: pd.DataFrame({"id": [1], "name": ["Bob"]}))

    runner = CliRunner()
    output_file = tmp_path / "out.csv"
    result = runner.invoke(cli, [
        "export",
        "--query", "SELECT id, name FROM users",
        "--output-file", str(output_file),
        "--format", "csv",
        "--export-mode", "sqlite-query",
        "--db-url", "sqlite:///test.db"
    ])
    assert result.exit_code == 0
    assert output_file.exists()

def test_matrix_config_from_env(tmp_path, monkeypatch):
    format = os.environ.get("FORMAT", "parquet")
    partitioned = os.environ.get("PARTITIONED", "false") == "true"
    output_path = tmp_path / "matrix_test"

    df = pd.DataFrame({
        "id": [1, 2],
        "group": ["A", "B"],
        "msg": ["foo", "bar"]
    })

    config = ExportJobConfig(
        query="SELECT * FROM dummy",
        db_url="sqlite://",
        export_mode=ExportMode("sqlite-query"),
        format=format,
        output_dir=output_path,
        partition_by=["group"] if partitioned else None,
    )

    run_export(config, fetch_override=lambda *_: df)
    if partitioned:
        assert (output_path / "group=A" / "part-0000.parquet").exists() or \
               (output_path / "group=A" / "part-0000.csv").exists()
    else:
        files = list(output_path.glob("output.*"))
        assert len(files) == 1
        assert files[0].exists()

def test_cli_partitioned_csv_empty_result(tmp_path, monkeypatch):
    monkeypatch.setattr("sqlxport.api.export.upload_file_to_s3", lambda *a, **kw: None)
    monkeypatch.setattr("sqlxport.api.export.fetch_query_as_dataframe", lambda *_: pd.DataFrame(columns=["id", "log_date", "msg"]))

    runner = CliRunner()
    output_dir = tmp_path / "empty_partitions"
    result = runner.invoke(cli, [
        "export",
        "--query", "SELECT * FROM logs",
        "--output-dir", str(output_dir),
        "--format", "csv",
        "--partition-by", "log_date",
        "--export-mode", "sqlite-query",
        "--db-url", "sqlite://"
    ])
    assert result.exit_code == 0
    assert not list(output_dir.rglob("*.csv"))

def test_cli_output_csv_non_partitioned(tmp_path, monkeypatch):
    monkeypatch.setattr("sqlxport.api.export.upload_file_to_s3", lambda *a, **kw: None)
    monkeypatch.setattr("sqlxport.api.export.fetch_query_as_dataframe", lambda *_: pd.DataFrame({"id": [1], "name": ["test"]}))

    runner = CliRunner()
    output_file = tmp_path / "out.csv"
    result = runner.invoke(cli, [
        "export",
        "--query", "SELECT * FROM dummy",
        "--output-file", str(output_file),
        "--format", "csv",
        "--export-mode", "sqlite-query",
        "--db-url", "sqlite://"
    ])
    assert result.exit_code == 0
    assert output_file.exists()
    df_read = pd.read_csv(output_file)
    assert df_read.to_dict(orient="records") == [{"id": 1, "name": "test"}]

def test_cli_partitioned_upload_s3(tmp_path, monkeypatch):
    df = pd.DataFrame({"id": [1, 2], "cat": ["X", "Y"]})
    monkeypatch.setattr("sqlxport.api.export.upload_file_to_s3", lambda *a, **kw: None)
    monkeypatch.setattr("sqlxport.api.export.fetch_query_as_dataframe", lambda *_: df)

    runner = CliRunner()
    output_dir = tmp_path / "partitioned_s3"
    result = runner.invoke(cli, [
        "export",
        "--query", "SELECT * FROM dummy",
        "--output-dir", str(output_dir),
        "--format", "parquet",
        "--partition-by", "cat",
        "--export-mode", "sqlite-query",
        "--db-url", "sqlite://",
        "--s3-output-prefix", "s3://fake/path"
    ])
    print("Result output:", result.output)
    for path in output_dir.rglob("*"):
        print(" -", path)

    assert result.exit_code == 0
    assert len(list((output_dir / "cat=X").glob("*.parquet"))) == 1
    assert len(list((output_dir / "cat=Y").glob("*.parquet"))) == 1
