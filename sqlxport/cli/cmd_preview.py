# sqlxport/cli/cmd_preview.py

import click
import json
import yaml
import pandas as pd
import glob
import os
from sqlalchemy import create_engine, text

from sqlxport.query_engines import get_query_engine
from sqlxport.core.storage import preview_s3_parquet, list_s3_objects


@click.command("preview")
@click.option('--local-file', help='Path to local Parquet or CSV file to preview')
@click.option('--local-dir', help='Path to local directory containing export files (e.g., Parquet partitioned output)')
@click.option('--s3-file', is_flag=True, help='Preview Parquet file from S3')
@click.option('--list-s3', is_flag=True, help='List objects in S3 prefix')
@click.option('--file-query-engine', default="duckdb", help='Query engine to use for preview (duckdb, athena, etc.)')
@click.option('--bucket', help='S3 bucket name')
@click.option('--key', help='S3 object key or prefix')
@click.option('--access-key', help='S3 access key')
@click.option('--secret-key', help='S3 secret key')
@click.option('--endpoint-url', help='S3 endpoint URL (useful for MinIO)')
@click.option('--engine-args', multiple=True, help='Extra args for query engine: key=value')
@click.option("--ai-summary", is_flag=True, help="Generate a natural-language summary of the file.")
def preview(local_file, local_dir, s3_file, list_s3, file_query_engine, bucket, key, access_key, secret_key, endpoint_url, engine_args, ai_summary):
    """Preview exported files or list S3 objects."""
    engine_kwargs = {}
    for item in engine_args:
        if '=' in item:
            k, v = item.split('=', 1)
            engine_kwargs[k.strip()] = v.strip()

    engine = get_query_engine(file_query_engine)

    if local_file:
        if ai_summary:
            from sqlxport.utils.summary import summarize_file
            print("\n📄 AI Summary:\n")
            print(summarize_file(local_file))
        else:
            print(engine.preview(local_file, **engine_kwargs))
        return

    if local_dir:
        all_files = glob.glob(os.path.join(local_dir, "**/*.parquet"), recursive=True)
        if not all_files:
            raise click.ClickException(f"No Parquet files found in {local_dir}")
        if ai_summary:
            from sqlxport.utils.summary import summarize_file
            for f in all_files:
                print(f"\n📄 AI Summary for {f}:")
                print(summarize_file(f))
        else:
            print(engine.preview(all_files, **engine_kwargs))
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

    raise click.UsageError("You must specify one of --local-file, --local-dir, --s3-file, or --list-s3")


@click.command("schema")
@click.option("--db-url", required=True, help="Database URL")
@click.option("--query", required=True, help="Query to preview")
@click.option("--as-json", is_flag=True, help="Output schema as JSON")
@click.option("--as-yaml", is_flag=True, help="Output schema as YAML")
def preview_schema(db_url, query, as_json, as_yaml):
    """Preview inferred schema from a query (column names & types)."""
    try:
        engine = create_engine(db_url)
        with engine.connect() as conn:
            df = pd.read_sql_query(text(f"SELECT * FROM ({query}) AS preview_subq LIMIT 0"), conn)
    except Exception as e:
        print(f"❌ Error during schema preview: {e}")
        raise click.ClickException(str(e))

    schema = [{"name": col, "type": str(dtype)} for col, dtype in zip(df.columns, df.dtypes)]

    if as_json:
        print(json.dumps(schema, indent=2))
    elif as_yaml:
        print(yaml.dump(schema, sort_keys=False))
    else:
        print("📄 Schema Preview:")
        for col in schema:
            print(f"  - {col['name']}: {col['type']}")
