# sqlxport/cli/cmd_export.py
import os
import posixpath
import click
from dotenv import load_dotenv

from sqlxport.api.export import run_export, ExportJobConfig, ExportMode
from sqlxport.core.export_modes import validate_export_mode
from sqlxport.core.s3_config import S3Config
from sqlxport.core.storage import upload_file_to_s3
from sqlxport.cli.utils import load_env_if_needed, echo_effective_config, validate_options, warn_if_s3_endpoint_suspicious


@click.command("export")
@click.option('--db-url', default=lambda: os.getenv("DB_URL"), help='Database connection URL')
@click.option('--query', required=True, help='SQL query to run')
@click.option('--output-file', required=False, help='Output file path')
@click.option('--output-dir', required=False, help='Directory to write partitioned output')
@click.option('--partition-by', required=False, help='Column to partition output by')
@click.option('--format', default="parquet", help='Output format (parquet, csv)')
@click.option('--export-mode', default=None, show_default=False, help='Export mode (auto-inferred if omitted): postgres-query, redshift-unload, etc.')
@click.option('--iam-role', default=lambda: os.getenv("IAM_ROLE"), help='IAM role for UNLOAD')
@click.option('--s3-output-prefix', default=lambda: os.getenv("S3_OUTPUT_PREFIX"), help='S3 path for UNLOAD output')
@click.option('--s3-bucket', default=lambda: os.getenv("S3_BUCKET"), help='S3 bucket')
@click.option('--s3-key', default=lambda: os.getenv("S3_KEY"), help='S3 object key or prefix')
@click.option('--s3-endpoint', default=lambda: os.getenv("S3_ENDPOINT"), help='S3 endpoint')
@click.option('--s3-access-key', default=lambda: os.getenv("S3_ACCESS_KEY"), help='S3 access key')
@click.option('--s3-secret-key', default=lambda: os.getenv("S3_SECRET_KEY"), help='S3 secret key')
@click.option('--aws-region', default=lambda: os.getenv("AWS_DEFAULT_REGION", "us-east-1"), help='AWS region')
@click.option('--upload-output-dir', is_flag=True, help='Upload all partitioned files to S3')
@click.option('--s3-provider', default="aws", type=click.Choice(["aws", "minio"], case_sensitive=False),
              help='Specify the S3 provider (aws or minio). Affects default behaviors and warnings.')
@click.option('--verbose', is_flag=True, help='Print debug output')
@click.option('--env-file', default=None, help='Optional .env file to load (e.g. tests/.env.test)')
def export(db_url, query, output_file, output_dir, partition_by, format, export_mode,
           iam_role, s3_output_prefix, s3_bucket, s3_key, s3_endpoint, s3_access_key,
           s3_secret_key, aws_region, upload_output_dir, s3_provider, verbose, env_file):
    print(f"query is {query}")
    
    load_env_if_needed(env_file)
    warn_if_s3_endpoint_suspicious(s3_endpoint, s3_provider)

    if verbose:
        echo_effective_config(locals())

    validate_options(db_url, query, output_file, output_dir, partition_by, None, None, None, format)
    validate_export_mode(export_mode=export_mode, db_type=db_url)

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
        export_mode=ExportMode(export_mode),
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
