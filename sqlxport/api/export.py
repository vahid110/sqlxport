# sqlxport/api/export.py

import click
from dataclasses import dataclass
from typing import Optional, List
from enum import Enum

from sqlxport.core.extract import fetch_query_as_dataframe
from sqlxport.formats.registry import get_writer
from sqlxport.core.storage import upload_file_to_s3
from sqlxport.redshift_unload import run_unload
from sqlxport.core.s3_config import S3Config




class ExportMode(str, Enum):
    QUERY = "query"
    UNLOAD = "unload"


@dataclass
class ExportJobConfig:
    db_url: str
    query: str
    output_file: Optional[str] = None
    output_dir: Optional[str] = None
    format: str = "parquet"
    partition_by: Optional[List[str]] = None
    export_mode: ExportMode = ExportMode.QUERY
    redshift_unload_role: Optional[str] = None
    s3_output_prefix: Optional[str] = None
    aws_profile: Optional[str] = None
    s3_config: Optional[S3Config] = None
    s3_upload: bool = False


def _validate_partition_column(df, partition_cols):
    missing = [col for col in partition_cols if col not in df.columns]
    if missing:
        raise ValueError(f"Missing partition column(s): {missing}")
    if df[partition_cols].isnull().any().any():
        raise ValueError("Partition column(s) contain null values")


def run_export(config: ExportJobConfig, fetch_override=None):
    # ✅ General validation for required CLI arguments
    if not config.export_mode:
        raise click.UsageError("Missing --export-mode")
    if not config.query:
        raise click.UsageError("Missing --query")
    if not config.format:
        raise click.UsageError("Missing --format")
    if not (config.output_file or config.output_dir or config.s3_output_prefix):
        raise click.UsageError("Missing output location. Provide --output-file, --output-dir, or --s3-output-prefix.")

    # ✅ UNLOAD-based export
    if config.export_mode == ExportMode.UNLOAD:
        if not config.redshift_unload_role:
            raise click.UsageError("Redshift UNLOAD requires --redshift-unload-role (IAM role).")
        if not config.s3_output_prefix:
            raise click.UsageError("Redshift UNLOAD requires --s3-output-prefix")

        run_unload(
            db_url=config.db_url,
            query=config.query,
            s3_output_prefix=config.s3_output_prefix,
            iam_role=config.redshift_unload_role,
            file_format=config.format
        )
        return config.s3_output_prefix

    # ✅ QUERY-based export
    fetch = fetch_override or fetch_query_as_dataframe
    df = fetch(config.db_url, config.query)

    if config.partition_by:
        _validate_partition_column(df, config.partition_by)

    writer = get_writer(config.format)
    output_path = writer(
        df,
        output_file=config.output_file,
        output_dir=config.output_dir,
        partition_by=config.partition_by
    )

    # ✅ Optional S3 upload
    if config.s3_upload:
        upload_file_to_s3(
            file_path=output_path,
            bucket_name=config.s3_config.bucket,
            object_key=config.s3_config.key,
            access_key=config.s3_config.access_key,
            secret_key=config.s3_config.secret_key,
            endpoint_url=config.s3_config.endpoint_url,
            region_name=config.s3_config.region_name
        )

    return output_path
