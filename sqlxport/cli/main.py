import click
import os
import sys
import posixpath
from dotenv import load_dotenv
from importlib.metadata import version, PackageNotFoundError

from sqlxport.core.extract import fetch_query_as_dataframe
from sqlxport.core.storage import upload_file_to_s3, list_s3_objects, preview_s3_parquet, preview_local_parquet
from sqlxport.redshift_unload import run_unload
from sqlxport.formats.registry import get_writer
from sqlxport.ddl.utils import generate_athena_ddl as build_athena_ddl

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

@click.command()
@click.option('--db-url', default=lambda: os.getenv("DB_URL"), help='PostgreSQL DB URL')
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
@click.option('--use-redshift-unload', is_flag=True, help='Use Redshift UNLOAD instead of SQL query')
@click.option('--iam-role', default=lambda: os.getenv("IAM_ROLE"), help='IAM role for UNLOAD')
@click.option('--s3-output-prefix', default=lambda: os.getenv("S3_OUTPUT_PREFIX"), help='S3 path for UNLOAD output')
@click.option('--list-s3-files', is_flag=True, help='List S3 files')
@click.option('--preview-s3-file', is_flag=True, help='Preview S3 parquet file')
@click.option('--preview-local-file', default=None, help='Preview local parquet file')
@click.option('--generate-athena-ddl', default=None, help='Generate Athena DDL for given file')
@click.option('--athena-s3-prefix', default=None, help='S3 prefix for Athena DDL generation')
@click.option('--athena-table-name', default="my_table", help='Table name for Athena DDL')
@click.option('--generate-env-template', is_flag=True, help='Write .env.example file')
@click.option('--verbose', is_flag=True, help='Print debug output')
@click.pass_context
def run(ctx, db_url, query, output_file, output_dir, partition_by, s3_bucket, s3_key, s3_endpoint,
        s3_access_key, s3_secret_key, aws_region, upload_output_dir, format,
        use_redshift_unload, iam_role, s3_output_prefix,
        list_s3_files, preview_s3_file, preview_local_file,
        generate_athena_ddl, athena_s3_prefix, athena_table_name,
        generate_env_template, verbose):

    load_dotenv()

    if "S3_ENDPOINT" in os.environ and not os.getenv("OVERRIDE_S3_ENDPOINT"):
        print(f"❌ Refusing to proceed: environment variable S3_ENDPOINT={os.getenv('S3_ENDPOINT')} may override AWS S3.")
        print(f"   To ignore this, unset S3_ENDPOINT or pass --s3-endpoint explicitly.")
        ctx.exit(1)

    if verbose:
        print("🔧 Effective config:")
        print(f"   Format: {format}")
        print(f"   S3 Bucket: {s3_bucket}")
        print(f"   S3 Key: {s3_key}")
        print(f"   S3 Endpoint: {s3_endpoint}")
        print(f"   s3_access_key: {s3_access_key}")
        print(f"   s3_secret_key: {bool(s3_secret_key)}")

    validate_options(
        db_url, query, output_file, output_dir, partition_by,
        generate_athena_ddl, athena_s3_prefix, athena_table_name, format
    )

    def warn_if_s3_endpoint_suspicious(endpoint):
        if endpoint and "amazonaws.com" not in endpoint:
            print(f"⚠️  Warning: Using custom S3 endpoint '{endpoint}'. If you're targeting AWS, this may be misconfigured.", flush=True)

    warn_if_s3_endpoint_suspicious(s3_endpoint)

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
        preview_local_parquet(preview_local_file)
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
        print("\n📜 Athena CREATE TABLE statement:\n")
        print(ddl)
        return

    if not query and not use_redshift_unload:
        raise click.UsageError("Missing required option '--query'.")

    writer = get_writer(format)

    if use_redshift_unload:
        run_unload(db_url, query, s3_output_prefix, iam_role)
        return

    df = fetch_query_as_dataframe(db_url, query)

    if output_dir:
        print(f"💾 Saving partitioned output to {output_dir}...")
        if partition_by:
            writer.write_partitioned(df, output_dir, partition_by)
        else:
            writer.write_flat(df, output_dir)

        if s3_bucket and s3_key:
            print("☁️ Uploading directory recursively to S3...")
            for root, _, files in os.walk(output_dir):
                for file in files:
                    full_path = os.path.join(root, file)
                    rel_path = os.path.relpath(full_path, output_dir)
                    object_key = posixpath.join(s3_key, rel_path)  # 🚨 fix here
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
    elif output_file:
        print(f"💾 Saving to {output_file}...")
        writer.write(df, output_file)
        if s3_bucket and s3_key and s3_access_key and s3_secret_key:
            print(f"☁️ Uploading to S3... {s3_bucket}")
            upload_file_to_s3(
                file_path=output_file,
                bucket_name=s3_bucket,
                object_key=s3_key,
                access_key=s3_access_key,
                secret_key=s3_secret_key,
                endpoint_url=s3_endpoint,
                region_name=aws_region
            )
    else:
        raise click.UsageError("Provide either --output-file or --output-dir.")

    print("✅ Done.")

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

cli.add_command(run)

if __name__ == "__main__":
    cli()
