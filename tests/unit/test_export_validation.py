import pytest
from sqlxport.api.export import ExportJobConfig, ExportMode, run_export
import click
import pandas as pd

def mock_fetch(_db_url, _query):
    return pd.DataFrame([{"a": 1}])

def test_missing_query_raises():
    config = ExportJobConfig(
        db_url="sqlite://",
        query=None,
        output_file="out.parquet",
        format="parquet",
        export_mode=ExportMode("sqlite-query")
    )
    with pytest.raises(click.UsageError, match="Missing --query"):
        run_export(config, fetch_override=mock_fetch)

def test_missing_export_mode_infers_correctly():
    config = ExportJobConfig(
        db_url="sqlite://",
        query="SELECT 1",
        output_file="out.parquet",
        format="parquet",
        export_mode=None
    )
    run_export(config, fetch_override=mock_fetch)

    # ✅ Assert it was inferred correctly
    assert config.export_mode == ExportMode("sqlite-query")


def test_missing_output_location_raises():
    config = ExportJobConfig(
        db_url="sqlite://",
        query="SELECT 1",
        format="parquet",
        export_mode=ExportMode("sqlite-query")
    )
    with pytest.raises(click.UsageError, match="Missing output location"):
        run_export(config, fetch_override=mock_fetch)

def test_redshift_unload_requires_iam_role():
    config = ExportJobConfig(
        db_url="redshift://host/dev",
        query="SELECT 1",
        export_mode=ExportMode("redshift-unload"),
        format="parquet",
        s3_output_prefix="s3://bucket/key"
    )
    with pytest.raises(click.UsageError, match="requires --iam-role"):
        run_export(config, fetch_override=mock_fetch)

def test_redshift_unload_requires_output_prefix():
    config = ExportJobConfig(
        db_url="redshift://host/dev",
        query="SELECT 1",
        export_mode=ExportMode("redshift-unload"),
        format="parquet",
        redshift_unload_role="arn:aws:iam::123:role/unload"
    )
    with pytest.raises(click.UsageError, match="Missing output location"):
        run_export(config, fetch_override=mock_fetch)
