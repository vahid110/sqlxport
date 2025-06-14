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
@click.option('--s3-url', required=True, help='S3 URL to use as LOCATION in DDL (e.g., s3://my-bucket/path)')
def generate_ddl(input_file, output_format, file_query_engine, partition_by, table_name, s3_url):
    """
    Generate CREATE TABLE DDL from a Parquet/CSV file or directory.

    Parameters:
        input-file: Local Parquet/CSV file or partitioned directory
        output-format: DDL dialect (athena only for now)
        file-query-engine: Schema inference engine (default: duckdb)
        partition-by: Comma-separated list of partition columns
        table-name: Output table name
        s3-url: S3 location for the table's LOCATION clause
    """
    if output_format != "athena":
        raise click.UsageError(f"DDL format '{output_format}' is not supported yet.")

    if not s3_url.startswith("s3://"):
        raise click.UsageError("--s3-url must be a valid S3 path, e.g. s3://my-bucket/path")

    engine = get_query_engine(file_query_engine)
    schema_df = engine.infer_schema(input_file)

    partition_cols = [col.strip() for col in partition_by.split(",")] if partition_by else None

    ddl = generate_athena_ddl(
        local_parquet_path=input_file,
        s3_prefix=s3_url,
        table_name=table_name,
        partition_cols=partition_cols,
        schema_df=schema_df
    )

    print(ddl)
