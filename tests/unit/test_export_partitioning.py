import pandas as pd
import pytest
from pathlib import Path
from sqlxport.api.export import ExportJobConfig, ExportMode, run_export


@pytest.fixture
def mock_fetch():
    return lambda *_: pd.DataFrame({
        "id": [1, 2, 3],
        "category": ["A", "B", "A"]
    })


def test_non_partitioned_output(tmp_path, mock_fetch):
    output_path = tmp_path / "flat.parquet"

    config = ExportJobConfig(
        query="SELECT * FROM dummy",
        db_url="sqlite://",
        export_mode=ExportMode("sqlite-query"),
        format="parquet",
        output_file=output_path,
    )

    run_export(config, fetch_override=mock_fetch)
    assert output_path.exists()


def test_partitioned_output(tmp_path, mock_fetch):
    output_dir = tmp_path / "partitioned"
    config = ExportJobConfig(
        query="SELECT * FROM dummy",
        db_url="sqlite://",
        export_mode=ExportMode("sqlite-query"),
        format="parquet",
        output_dir=output_dir,
        partition_by=["category"]
    )

    run_export(config, fetch_override=mock_fetch)

    cat_a = list((output_dir / "category=A").glob("*.parquet"))
    cat_b = list((output_dir / "category=B").glob("*.parquet"))

    assert len(cat_a) == 1
    assert cat_a[0].exists()
    assert len(cat_b) == 1
    assert cat_b[0].exists()


def test_partition_column_missing_raises(tmp_path):
    df = pd.DataFrame({"x": [1, 2], "y": ["foo", "bar"]})
    mock_fetch = lambda *_: df
    config = ExportJobConfig(
        query="SELECT * FROM dummy",
        db_url="sqlite://",
        export_mode=ExportMode("sqlite-query"),
        format="parquet",
        output_dir=tmp_path,
        partition_by=["nonexistent"]
    )
    with pytest.raises(ValueError, match=r"Missing partition column\(s\): \['nonexistent'\]"):
        run_export(config, fetch_override=mock_fetch)


def test_partitioning_on_empty_dataframe(tmp_path):
    mock_fetch = lambda *_: pd.DataFrame(columns=["category", "id"])
    config = ExportJobConfig(
        query="SELECT * FROM dummy",
        db_url="sqlite://",
        export_mode=ExportMode("sqlite-query"),
        format="parquet",
        output_dir=tmp_path,
        partition_by=["category"]
    )
    run_export(config, fetch_override=mock_fetch)
    files = list(tmp_path.rglob("*.parquet"))
    assert len(files) == 0

def test_partitioned_single_value(tmp_path):
    mock_fetch = lambda *_: pd.DataFrame({
        "id": [1, 2, 3],
        "category": ["only"] * 3
    })
    output_dir = tmp_path / "single_val"
    config = ExportJobConfig(
        query="SELECT * FROM dummy",
        db_url="sqlite://",
        export_mode=ExportMode("sqlite-query"),
        format="parquet",
        output_dir=output_dir,
        partition_by=["category"]
    )

    run_export(config, fetch_override=mock_fetch)
    partition_files = list((output_dir / "category=only").glob("*.parquet"))
    assert len(partition_files) == 1

def test_partitioning_with_nulls(tmp_path):
    mock_fetch = lambda *_: pd.DataFrame({
        "id": [1, 2, 3, 4],
        "category": ["A", None, "B", None]
    })
    output_dir = tmp_path / "nulls"
    config = ExportJobConfig(
        query="SELECT * FROM dummy",
        db_url="sqlite://",
        export_mode=ExportMode("sqlite-query"),
        format="parquet",
        output_dir=output_dir,
        partition_by=["category"]
    )

    with pytest.raises(ValueError, match="contain null values"):
        run_export(config, fetch_override=mock_fetch)