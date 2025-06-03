import os
import sqlite3
import pytest
import tempfile
import pandas as pd
from click.testing import CliRunner
from sqlxport.cli.main import cli
from unittest.mock import MagicMock

def create_sample_sqlite_db(with_partition_column=False):
    conn = sqlite3.connect(":memory:")
    if with_partition_column:
        df = pd.DataFrame({
            "id": [1, 2],
            "log_date": ["2024-05-01", "2024-05-02"],
            "msg": ["foo", "bar"]
        })
        df.to_sql("logs", conn, index=False)
    else:
        df = pd.DataFrame({
            "id": [1, 2],
            "name": ["Alice", "Bob"]
        })
        df.to_sql("users", conn, index=False)
    return conn

def test_cli_output_file():
    runner = CliRunner()

    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmpfile:
        output_file = tmpfile.name

    db_url = "sqlite://"

    result = runner.invoke(cli, [
        "run",
        "--db-url", db_url,
        "--query", "SELECT * FROM users",
        "--output-file", output_file,
        "--format", "parquet"
    ], catch_exceptions=False)

    assert result.exit_code == 0


@pytest.fixture
def runner():
    return CliRunner()


def test_root_help_command(runner):
    result = runner.invoke(cli, ['--help'])
    assert result.exit_code == 0
    assert "run" in result.output

def test_run_help_command(runner):
    result = runner.invoke(cli, ['run', '--help'])
    assert result.exit_code == 0
    assert "--query" in result.output



def test_missing_query_error(runner):
    result = runner.invoke(cli, ['run', '--output-file', 'dummy.parquet'])
    assert result.exit_code != 0
    assert "Missing required option '--query'" in result.output


def test_redshift_unload_requires_iam_role(runner):
    result = runner.invoke(cli, [
        'run',
        '--query', 'SELECT 1',
        '--use-redshift-unload'
    ])
    assert result.exit_code != 0
    assert "IAM role" in result.output or "IAM_ROLE" in result.output


def test_preview_local_file_invalid_path(runner):
    result = runner.invoke(cli, [
        'run',
        '--preview-local-file', 'nonexistent.parquet'
    ])
    assert "Failed to read" in result.output or "No such file" in result.output


def test_invalid_combo_output_file_with_partitioned_dir(runner):
    result = runner.invoke(cli, [
        'run',
        '--query', 'SELECT 1',
        '--output-file', 'out.parquet',
        '--output-dir', 'outdir'
    ])
    assert result.exit_code != 0
    assert "only one of --output-file or --output-dir" in result.output or "Usage" in result.output


def test_cli_output_csv_file(tmp_path):
    runner = CliRunner()

    output_file = tmp_path / "users.csv"
    conn = create_sample_sqlite_db()
    db_url = "sqlite://"

    result = runner.invoke(cli, [
        "run",
        "--db-url", db_url,
        "--query", "SELECT * FROM users",
        "--output-file", str(output_file),
        "--format", "csv"
    ])

    assert result.exit_code == 0
    assert output_file.exists()
    contents = output_file.read_text()
    assert "Alice" in contents and "Bob" in contents


def test_cli_output_csv_partitioned(tmp_path, monkeypatch):
    import pandas as pd

    runner = CliRunner()
    output_dir = tmp_path / "csv_parts"

    df = pd.DataFrame({
        "id": [1, 2],
        "log_date": ["2024-05-01", "2024-05-02"],
        "msg": ["foo", "bar"]
    })

    monkeypatch.setattr("sqlxport.cli.main.fetch_query_as_dataframe", lambda *_: df)
    monkeypatch.setattr("sqlxport.cli.main.upload_file_to_s3", lambda *args, **kwargs: None)

    result = runner.invoke(cli, [
        "run",
        "--db-url", "sqlite://",
        "--query", "SELECT * FROM logs",
        "--output-dir", str(output_dir),
        "--partition-by", "log_date",
        "--format", "csv"
    ])

    assert result.exit_code == 0


def test_invalid_format_fails(tmp_path):
    runner = CliRunner()

    result = runner.invoke(cli, [
        "run",
        "--db-url", "sqlite://",
        "--query", "SELECT 1",
        "--output-file", str(tmp_path / "out.xyz"),
        "--format", "xyz"
    ])

    assert result.exit_code != 0
    assert "Unsupported format 'xyz'. Supported formats are: parquet, csv." in result.output


def test_cli_partitioned_csv_contents(tmp_path, monkeypatch):
    import pandas as pd

    runner = CliRunner()
    output_dir = tmp_path / "csv_parts"

    df = pd.DataFrame({
        "id": [1, 2, 3],
        "log_date": ["2024-01-01", "2024-01-02", "2024-01-01"],
        "msg": ["foo", "bar", "baz"]
    })

    monkeypatch.setattr("sqlxport.cli.main.fetch_query_as_dataframe", lambda *_: df)
    monkeypatch.setattr("sqlxport.cli.main.upload_file_to_s3", lambda *args, **kwargs: None)  # 🧯 disable actual upload

    result = runner.invoke(cli, [
        "run",
        "--db-url", "sqlite://",
        "--query", "SELECT * FROM logs",
        "--output-dir", str(output_dir),
        "--partition-by", "log_date",
        "--format", "csv"
    ])

    print(result.output)
    assert result.exit_code == 0



def test_cli_partitioned_csv_empty_result(tmp_path, monkeypatch):
    import pandas as pd

    monkeypatch.setattr("sqlxport.cli.main.fetch_query_as_dataframe", lambda *_: pd.DataFrame(columns=["id", "log_date", "msg"]))

    runner = CliRunner()
    output_dir = tmp_path / "csv_parts"

    result = runner.invoke(cli, [
        "run",
        "--db-url", "sqlite://",
        "--query", "SELECT * FROM logs",
        "--output-dir", str(output_dir),
        "--partition-by", "log_date",
        "--format", "csv"
    ])

    print(result.output)
    assert result.exit_code == 0
    assert output_dir.exists()
    assert len(list(output_dir.iterdir())) == 0


def test_cli_output_csv_non_partitioned(tmp_path, monkeypatch):
    import pandas as pd
    import sqlxport.cli.main as cli_module

    df = pd.DataFrame({
        "id": [1, 2],
        "msg": ["foo", "bar"]
    })

    monkeypatch.setattr(cli_module, "fetch_query_as_dataframe", lambda *_: df)

    runner = CliRunner()
    output_file = tmp_path / "simple_output.csv"

    result = runner.invoke(cli, [
        "run",
        "--db-url", "sqlite://",
        "--query", "SELECT * FROM dummy",
        "--output-file", str(output_file),
        "--format", "csv"
    ])

    assert result.exit_code == 0
    assert output_file.exists()

    df_out = pd.read_csv(output_file)
    pd.testing.assert_frame_equal(df, df_out)


def test_cli_partitioned_upload_s3(tmp_path, monkeypatch):
    import pandas as pd

    monkeypatch.setattr("sqlxport.cli.main.fetch_query_as_dataframe", lambda *_: pd.DataFrame({
        "id": [1, 2],
        "cat": ["X", "Y"]
    }))

    mock_upload = MagicMock()
    monkeypatch.setattr("sqlxport.cli.main.upload_file_to_s3", mock_upload)

    monkeypatch.setattr("os.walk", lambda path: [
        (f"{path}/cat=X", [], ["part-0000.csv"]),
        (f"{path}/cat=Y", [], ["part-0000.csv"]),
    ])

    runner = CliRunner()
    output_dir = tmp_path / "out"
    result = runner.invoke(cli, [
        "run",
        "--db-url", "sqlite://",
        "--query", "SELECT * FROM dummy",
        "--output-dir", str(output_dir),
        "--partition-by", "cat",
        "--s3-bucket", "my-bucket",
        "--s3-key", "my-prefix",
        "--s3-access-key", "abc",
        "--s3-secret-key", "xyz",
        "--upload-output-dir",
        "--format", "csv"
    ])

    assert result.exit_code == 0
    assert mock_upload.call_count == 2
