# sqlxport/cli/main.py
import click
import os
import sys
import posixpath
from dotenv import load_dotenv
from importlib.metadata import version, PackageNotFoundError

from sqlxport.core.storage import upload_file_to_s3, list_s3_objects, preview_s3_parquet
from sqlxport.ddl.utils import generate_athena_ddl as build_athena_ddl
from sqlxport.api.export import run_export, ExportJobConfig, ExportMode
from sqlxport.core.export_modes import validate_export_mode
from sqlxport.query_engines import get_query_engine
from sqlxport.core.s3_config import S3Config

try:
    __version__ = version("sqlxport")
except PackageNotFoundError:
    __version__ = "unknown"

def validate_options(db_url, query, output_file, output_dir, partition_by,
                     generate_athena_ddl, athena_s3_prefix, athena_table_name,
                     format):
    if output_file and output_dir:
        raise click.UsageError("You must provide either --output-file or --output-dir, not both.")
    if partition_by and not output_dir:
        raise click.UsageError("--partition-by requires --output-dir.")
    if generate_athena_ddl and not (athena_s3_prefix and athena_table_name):
        raise click.UsageError("--generate-athena-ddl requires both --athena-s3-prefix and --athena-table-name.")
    if format not in {"parquet", "csv"}:
        raise click.UsageError(f"Unsupported format '{format}'. Supported formats are: parquet, csv.")

def warn_if_s3_endpoint_suspicious(endpoint, provider):
    if provider == "aws" and endpoint and "amazonaws.com" not in endpoint:
        print(f"⚠️  Warning: Using custom S3 endpoint '{endpoint}' while provider is set to 'aws'.", flush=True)
    elif provider == "minio" and not endpoint:
        print(f"⚠️  Warning: No --s3-endpoint specified while using 'minio' provider.", flush=True)

@click.command()
@click.option('--db-url', default=lambda: os.getenv("DB_URL"), help='Database connection URL')
@click.option('--query', required=False, help='SQL query to run')
@click.option('--output-file', required=False, help='Output file path')
@click.option('--output-dir', required=False, help='Directory to write partitioned output')
@click.option('--partition-by', required=False, help='Column to partition output by')
@click.option('--s3-bucket', default=lambda: os.getenv("S3_BUCKET"), help='S3 bucket')
@click.option('--s3-key', default=lambda: os.getenv("S3_KEY"), help='S3 object key or prefix')
@click.option('--s3-endpoint', default=lambda: os.getenv("S3_ENDPOINT"), help='S3 endpoint')
@click.option('--s3-access-key', default=lambda: os.getenv("S3_ACCESS_KEY"), help='S3 access key')
@click.option('--s3-secret-key', default=lambda: os.getenv("S3_SECRET_KEY"), help='S3 secret key')
@click.option('--aws-region', default=lambda: os.getenv("AWS_DEFAULT_REGION", "us-east-1"), help='AWS region')
@click.option('--upload-output-dir', is_flag=True, help='Upload all partitioned files to S3')
@click.option('--format', default="parquet", help='Output format (parquet, csv)')
@click.option('--export-mode', required=False, help='Export mode: query (default), or redshift-unload')
@click.option('--iam-role', default=lambda: os.getenv("IAM_ROLE"), help='IAM role for UNLOAD')
@click.option('--s3-output-prefix', default=lambda: os.getenv("S3_OUTPUT_PREFIX"), help='S3 path for UNLOAD output')
@click.option('--list-s3-files', is_flag=True, help='List S3 files')
@click.option('--preview-s3-file', is_flag=True, help='Preview S3 parquet file')
@click.option('--preview-local-file', default=None, help='Preview local parquet file')
@click.option('--generate-athena-ddl', default=None, help='Generate Athena DDL for given file')
@click.option('--athena-s3-prefix', default=None, help='S3 prefix for Athena DDL generation')
@click.option('--athena-table-name', default="my_table", help='Table name for Athena DDL')
@click.option('--generate-env-template', is_flag=True, help='Write .env.example file')
@click.option('--file-query-engine', default="duckdb", help="Engine to preview exported files (duckdb, athena, trino)")
@click.option('--verbose', is_flag=True, help='Print debug output')
@click.option('--s3-provider', default="aws", type=click.Choice(["aws", "minio"], case_sensitive=False),
              help='Specify the S3 provider (aws or minio). Affects default behaviors and warnings.')
@click.option('--env-file', default=None, help='Optional .env file to load (e.g. tests/.env.test)')
@click.pass_context
def run(ctx, db_url, query, output_file, output_dir, partition_by, s3_bucket, s3_key, s3_endpoint,
        s3_access_key, s3_secret_key, aws_region, upload_output_dir, format,
        export_mode, iam_role, s3_output_prefix,
        list_s3_files, preview_s3_file, preview_local_file,
        generate_athena_ddl, athena_s3_prefix, athena_table_name,
        generate_env_template, file_query_engine, verbose, s3_provider, env_file):

    # Only load .env file if explicitly requested
    dotenv_path = os.getenv("SQLXPORT_ENV_PATH")
    if dotenv_path:
        print(f"🔍 Loading environment overrides from {dotenv_path}")
        load_dotenv(dotenv_path=dotenv_path)
    if env_file:
        print(f"🔍 Loading environment overrides from {env_file}")
        load_dotenv(dotenv_path=env_file)



    if "S3_ENDPOINT" in os.environ and not os.getenv("OVERRIDE_S3_ENDPOINT"):
        print(f"❌ Refusing to proceed: environment variable S3_ENDPOINT={os.getenv('S3_ENDPOINT')} may override AWS S3.")
        print(f"   To ignore this, unset S3_ENDPOINT or pass --s3-endpoint explicitly.")
        ctx.exit(1)

    if verbose:
        print("🔧 Effective config:")
        print(f"   Format: {format}")
        print(f"   Export Mode: {export_mode}")
        print(f"   File Query Engine: {file_query_engine}")
        print(f"   S3 Bucket: {s3_bucket}")
        print(f"   S3 Key: {s3_key}")
        print(f"   S3 Endpoint: {s3_endpoint}")

    validate_options(
        db_url, query, output_file, output_dir, partition_by,
        generate_athena_ddl, athena_s3_prefix, athena_table_name, format
    )

    validate_export_mode(export_mode=export_mode, db_type=db_url)
    warn_if_s3_endpoint_suspicious(s3_endpoint, s3_provider)

    if generate_env_template:
        with open(".env.example", "w") as f:
            f.write("""\
DB_URL=postgresql://username:password@localhost:5432/dbname
S3_BUCKET=data-exports
S3_KEY=users.parquet
S3_ENDPOINT=http://localhost:9000
S3_ACCESS_KEY=minioadmin
S3_SECRET_KEY=minioadmin
IAM_ROLE=arn:aws:iam::123456789012:role/MyUnloadRole
S3_OUTPUT_PREFIX=s3://data-exports/unload/
""")
        print("✅ .env.example template generated.")
        return

    if preview_local_file:
        engine = get_query_engine(file_query_engine)
        print(engine.preview(preview_local_file))
        return

    if preview_s3_file:
        preview_s3_parquet(
            bucket=s3_bucket,
            key=s3_key,
            access_key=s3_access_key,
            secret_key=s3_secret_key,
            endpoint_url=s3_endpoint
        )
        return

    if list_s3_files:
        list_s3_objects(
            bucket=s3_bucket,
            prefix=s3_key,
            access_key=s3_access_key,
            secret_key=s3_secret_key,
            endpoint_url=s3_endpoint
        )
        return

    if generate_athena_ddl:
        if not athena_s3_prefix:
            raise click.UsageError("Missing --athena-s3-prefix")
        ddl = build_athena_ddl(generate_athena_ddl, athena_s3_prefix, athena_table_name,
                               partition_cols=[partition_by] if partition_by else None)
        print(ddl)
        return

    if not query:
        raise click.UsageError("Missing required option '--query'.")
    s3_cfg = S3Config(
        bucket=s3_bucket,
        key=s3_key,
        access_key=s3_access_key,
        secret_key=s3_secret_key,
        endpoint_url=s3_endpoint,
        region_name=aws_region
    )

    config = ExportJobConfig(
        db_url=db_url,
        query=query,
        output_file=output_file,
        output_dir=output_dir,
        format=format,
        partition_by=[partition_by] if partition_by else None,
        export_mode=ExportMode.UNLOAD if export_mode == "redshift-unload" else ExportMode.QUERY,
        redshift_unload_role=iam_role,
        s3_output_prefix=s3_output_prefix,
        aws_profile=None,
        s3_config=s3_cfg,
        s3_upload=bool(s3_bucket and s3_key)
    )

    output_path = run_export(config)

    if upload_output_dir and output_dir and s3_bucket and s3_key:
        print("☁️ Uploading directory recursively to S3...")
        for root, _, files in os.walk(output_dir):
            for file in files:
                full_path = os.path.join(root, file)
                rel_path = os.path.relpath(full_path, output_dir)
                object_key = posixpath.join(s3_key, rel_path)
                print(f"   • {rel_path}")
                upload_file_to_s3(
                    file_path=full_path,
                    bucket_name=s3_bucket,
                    object_key=object_key,
                    access_key=s3_access_key,
                    secret_key=s3_secret_key,
                    endpoint_url=s3_endpoint,
                    region_name=aws_region
                )
        print("✅ Folder upload complete.")

    print(f"✅ Export complete. Output: {output_path}")

@click.group(invoke_without_command=True)
@click.version_option(__version__, "--version", "-v", message="%(version)s")
@click.pass_context
def cli(ctx):
    """sqlxport CLI entrypoint"""
    if ctx.invoked_subcommand is None:
        ctx.invoke(run)

@cli.command()
@click.pass_context
def help(ctx):
    click.echo(run.get_help(ctx))

@cli.command("generate-ddl")
@click.option('--input-file', required=True, help='Path to input file (Parquet/CSV)')
@click.option('--output-format', default="athena", help='Target DDL dialect (e.g., athena)')
@click.option('--file-query-engine', default="duckdb", help='Engine to use for schema inference (duckdb, athena, etc.)')
@click.option('--partition-by', default=None, help='Comma-separated partition columns')
def generate_ddl(input_file, output_format, file_query_engine, partition_by):
    from sqlxport.query_engines import get_query_engine

    engine = get_query_engine(file_query_engine)
    schema_df = engine.infer_schema(input_file)

    if output_format != "athena":
        raise click.UsageError(f"DDL format '{output_format}' is not supported yet.")

    partition_cols = [col.strip() for col in partition_by.split(",")] if partition_by else None

    from sqlxport.ddl.utils import generate_athena_ddl
    ddl = generate_athena_ddl(input_file, input_file, "my_table", partition_cols, schema_df=schema_df)
    print(ddl)

@cli.command("validate-table")
@click.option('--table-name', required=True, help='Fully qualified table name')
@click.option('--file-query-engine', default="duckdb", help='Engine to use for validation')
@click.option('--athena-output-location', required=False, help="Athena query result location")
def validate_table(table_name, file_query_engine, athena_output_location):
    from sqlxport.query_engines import get_query_engine

    engine = get_query_engine(file_query_engine)

    kwargs = {}
    if file_query_engine == "athena":
        kwargs["output_location"] = athena_output_location

    engine.validate_table(table_name, **kwargs)

def export(**kwargs) -> str:
    """
    Lightweight alternative to run_export for quick use in notebooks or scripts.

    Accepts all fields from ExportJobConfig as keyword arguments.
    """
    config = ExportJobConfig(**kwargs)
    return run_export(config)

cli.add_command(run)
cli.add_command(generate_ddl)
cli.add_command(validate_table) 

if __name__ == "__main__":
    cli()
