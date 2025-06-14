# sqlxport/cli/cmd_validate_table.py

import click
from sqlxport.query_engines import get_query_engine

@click.command("validate-table")
@click.option('--table-name', required=True, help='Fully qualified table name')
@click.option('--file-query-engine', default="duckdb", help='Engine to use for validation (duckdb, athena, etc.)')
@click.option('--athena-output-location', required=False, help="S3 output location for Athena queries")
def validate_table(table_name, file_query_engine, athena_output_location):
    """Run SELECT COUNT(*) and/or partition validation on a table."""
    engine = get_query_engine(file_query_engine)

    kwargs = {}
    if file_query_engine == "athena":
        if not athena_output_location:
            raise click.UsageError("--athena-output-location is required for Athena validation.")
        kwargs["output_location"] = athena_output_location

    result = engine.validate_table(table_name, **kwargs)
    print(result)
