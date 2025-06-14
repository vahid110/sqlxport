import click


def validate_options(db_url, query, output_file, output_dir, partition_by):
    if output_file and output_dir:
        raise click.UsageError("You must provide either --output-file or --output-dir, not both.")
    if partition_by and not output_dir:
        raise click.UsageError("--partition-by requires --output-dir.")
    if not query:
        raise click.UsageError("Missing required option '--query'.")
    if not db_url:
        raise click.UsageError("Missing required option '--db-url'")


def warn_if_s3_endpoint_suspicious(endpoint, provider):
    if provider == "aws" and endpoint and "amazonaws.com" not in endpoint:
        print(f"⚠️  Warning: Using custom S3 endpoint '{endpoint}' while provider is set to 'aws'.", flush=True)
    elif provider == "minio" and not endpoint:
        print(f"⚠️  Warning: No --s3-endpoint specified while using 'minio' provider.", flush=True)
