# sqlxport/cli/utils.py
import os
from dotenv import load_dotenv
import click


def load_env_if_needed(env_file):
    dotenv_path = os.getenv("SQLXPORT_ENV_PATH")
    if dotenv_path:
        print(f"🔍 Loading environment overrides from {dotenv_path}")
        load_dotenv(dotenv_path=dotenv_path)
    if env_file:
        print(f"🔍 Loading environment overrides from {env_file}")
        load_dotenv(dotenv_path=env_file)


def echo_effective_config(config: dict):
    print("🔧 Effective config:")
    for key in sorted(config):
        print(f"   {key}: {config[key]}")


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
