# sqlxport/cli/cmd_generate_ddl.py

import click
from sqlxport.query_engines import get_query_engine
from sqlxport.ddl.utils import generate_athena_ddl

@click.command("generate-ddl")
@click.option('--input-file', required=True, help='Path to input file (Parquet/CSV)')
@click.option('--output-format', default="athena", help='Target DDL dialect (e.g., athena)')
@click.option('--file-query-engine', default="duckdb", help='Engine to use for schema inference (duckdb, athena, etc.)')
@click.option('--partition-by', default=None, help='Comma-separated partition columns')
@click.option('--table-name', required=True, help='Table name for DDL')
def generate_ddl(input_file, output_format, file_query_engine, partition_by, table_name, s3_url=None):
    """
    Generate CREATE TABLE DDL from a Parquet/CSV file or directory.

    Parameters:
        input_file (str): Local path to a Parquet/CSV file or partitioned folder.
        output_format (str): DDL dialect to generate (e.g., 'athena').
        file_query_engine (str): Engine used to infer schema ('duckdb', etc.).
        partition_by (str): Optional comma-separated list of partition columns.
        table_name (str): Name of the target table.
        s3_url (str): S3 URL to use in LOCATION clause (defaults to input_file if not given).
    """
    if not input_file or not table_name:
        raise click.UsageError("Both --local-path and --table-name are required.")

    if output_format != "athena":
        raise click.UsageError(f"DDL format '{output_format}' is not supported yet.")

    engine = get_query_engine(file_query_engine)
    schema_df = engine.infer_schema(input_file)
    partition_cols = [col.strip() for col in partition_by.split(",")] if partition_by else None

    ddl = generate_athena_ddl(
        local_parquet_path=input_file,
        s3_prefix=s3_url or input_file,
        table_name=table_name,
        partition_cols=partition_cols,
        schema_df=schema_df
    )
    print(ddl)

