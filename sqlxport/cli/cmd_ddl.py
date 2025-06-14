import click
from sqlxport.query_engines import get_query_engine
from sqlxport.ddl.utils import generate_athena_ddl


@click.command("ddl")
@click.option('--input-file', required=True, help='Path to input file (Parquet/CSV)')
@click.option('--output-format', default="athena", help='Target DDL dialect (default: athena)')
@click.option('--file-query-engine', default="duckdb", help='Engine to use for schema inference (duckdb, athena, etc.)')
@click.option('--partition-by', default=None, help='Comma-separated partition columns')
@click.option('--table-name', default="my_table", help='Target table name for DDL')
def ddl(input_file, output_format, file_query_engine, partition_by, table_name):
    """Generate DDL (Athena-compatible) for a given file"""
    engine = get_query_engine(file_query_engine)
    schema_df = engine.infer_schema(input_file)

    if output_format != "athena":
        raise click.UsageError(f"DDL format '{output_format}' is not supported yet.")

    partition_cols = [col.strip() for col in partition_by.split(",")] if partition_by else None

    ddl = generate_athena_ddl(
        file_path=input_file,
        s3_prefix=input_file,  # We reuse local path for now
        table_name=table_name,
        partition_cols=partition_cols,
        schema_df=schema_df
    )
    print(ddl)
