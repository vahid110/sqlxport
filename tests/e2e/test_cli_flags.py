# tests/e2e/test_cli_flags.py

from sqlxport.cli.main import cli
import tempfile
import pyarrow as pa
import pyarrow.parquet as pq
from sqlxport.cli.main import cli

def test_conflicting_output_flags(cli_runner):
    result = cli_runner.invoke(cli, [
        "export",
        "--db-url", "sqlite://",
        "--query", "SELECT 1",
        "--output-file", "out.parquet",
        "--output-dir", "out/"
    ])
    assert result.exit_code == 2
    assert "either --output-file or --output-dir, not both" in result.output


def test_missing_query_error(cli_runner):
    result = cli_runner.invoke(cli, ["export", "--db-url", "sqlite://"])
    assert result.exit_code == 2
    assert "Missing option '--query'" in result.output


def test_partition_by_without_output_dir(cli_runner):
    result = cli_runner.invoke(cli, [
        "export",
        "--db-url", "sqlite://",
        "--query", "SELECT * FROM dummy",
        "--partition-by", "col"
    ])
    assert result.exit_code == 2
    assert "--partition-by requires --output-dir" in result.output


def test_invalid_format(cli_runner):
    result = cli_runner.invoke(cli, [
        "export",
        "--db-url", "sqlite://",
        "--query", "SELECT * FROM dummy",
        "--output-file", "out.xyz",
        "--format", "xls"
    ])
    assert result.exit_code == 2
    assert "Unsupported format" in result.output


def test_generate_ddl_missing_params(cli_runner):
    table = pa.table({"id": [1, 2, 3]})
    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
        pq.write_table(table, tmp.name)
        input_file = tmp.name

    # Adjust flags based on real CLI
    result = cli_runner.invoke(cli, [
        "generate-ddl",
        "--input-file", input_file  # <- use real flag
        # no --table-name
    ])
    assert result.exit_code == 2
    assert "Missing option '--table-name'" in result.output  # <- use real error string

def test_ai_summary_flag(cli_runner, sample_parquet_file):
    result = cli_runner.invoke(
        cli,
        ["preview", "--local-file", sample_parquet_file, "--ai-summary"]
    )
    print("\n--- CLI OUTPUT ---")
    print(result.output)
    print("------------------")

    assert result.exit_code == 0




