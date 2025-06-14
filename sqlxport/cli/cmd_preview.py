import click
from sqlxport.query_engines import get_query_engine
from sqlxport.core.storage import preview_s3_parquet, list_s3_objects


@click.command("preview")
@click.option('--local-file', help='Path to local Parquet or CSV file to preview')
@click.option('--s3-file', is_flag=True, help='Preview Parquet file from S3')
@click.option('--list-s3', is_flag=True, help='List objects in S3 prefix')
@click.option('--file-query-engine', default="duckdb", help='Query engine to use for preview (duckdb, athena, etc.)')
@click.option('--bucket', help='S3 bucket name')
@click.option('--key', help='S3 object key or prefix')
@click.option('--access-key', help='S3 access key')
@click.option('--secret-key', help='S3 secret key')
@click.option('--endpoint-url', help='S3 endpoint URL (useful for MinIO)')
def preview(local_file, s3_file, list_s3, file_query_engine, bucket, key, access_key, secret_key, endpoint_url):
    """Preview exported files or list S3 objects."""
    if local_file:
        engine = get_query_engine(file_query_engine)
        print(engine.preview(local_file))
        return

    if s3_file:
        if not (bucket and key and access_key and secret_key):
            raise click.UsageError("Missing required S3 options for --s3-file")
        preview_s3_parquet(
            bucket=bucket,
            key=key,
            access_key=access_key,
            secret_key=secret_key,
            endpoint_url=endpoint_url
        )
        return

    if list_s3:
        if not (bucket and key and access_key and secret_key):
            raise click.UsageError("Missing required S3 options for --list-s3")
        list_s3_objects(
            bucket=bucket,
            prefix=key,
            access_key=access_key,
            secret_key=secret_key,
            endpoint_url=endpoint_url
        )
        return

    raise click.UsageError("You must specify one of --local-file, --s3-file, or --list-s3")
