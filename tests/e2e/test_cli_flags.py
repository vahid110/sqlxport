# tests/e2e/test_cli_flags.py

from sqlxport.cli.main import cli

def test_conflicting_output_flags(cli_runner):
    result = cli_runner.invoke(cli, [
        "run",
        "--db-url", "sqlite://",
        "--query", "SELECT 1",
        "--output-file", "out.parquet",
        "--output-dir", "out/"
    ])
    assert result.exit_code == 2
    assert "either --output-file or --output-dir, not both" in result.output

def test_missing_query_error(cli_runner):
    result = cli_runner.invoke(cli, ["run", "--db-url", "sqlite://"])
    assert result.exit_code == 2
    assert "Missing required option '--query'" in result.output

def test_partition_by_without_output_dir(cli_runner):
    result = cli_runner.invoke(cli, [
        "run",
        "--db-url", "sqlite://",
        "--query", "SELECT * FROM dummy",
        "--partition-by", "col"
    ])
    assert result.exit_code == 2
    assert "--partition-by requires --output-dir" in result.output

def test_invalid_format(cli_runner):
    result = cli_runner.invoke(cli, [
        "run",
        "--db-url", "sqlite://",
        "--query", "SELECT * FROM dummy",
        "--output-file", "out.xyz",
        "--format", "xls"
    ])
    assert result.exit_code == 2
    assert "Unsupported format" in result.output

def test_generate_ddl_missing_params(cli_runner):
    result = cli_runner.invoke(cli, [
        "run",
        "--generate-athena-ddl", "file.parquet"
    ])
    assert result.exit_code == 2
    assert "--generate-athena-ddl requires both" in result.output
