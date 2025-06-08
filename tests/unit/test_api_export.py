import os
import tempfile
import pandas as pd
import pytest
from sqlalchemy import create_engine, text
from sqlxport.formats.base import FormatWriter


from sqlxport.api.export import ExportJobConfig, run_export

@pytest.fixture
def sample_sqlite_db():
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
        db_path = tmp.name

    db_url = f"sqlite:///{db_path}"
    engine = create_engine(db_url)

    # Create a sample table
    df = pd.DataFrame({
        "id": [1, 2, 3],
        "category": ["A", "B", "A"],
        "value": [10, 20, 30]
    })

    with engine.begin() as conn:
        df.to_sql("sample_table", con=conn, index=False, if_exists="replace")

    yield db_url, db_path

    if os.path.exists(db_path):
        os.remove(db_path)

def test_run_export_parquet_file(sample_sqlite_db):
    db_url, _ = sample_sqlite_db
    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmpfile:
        output_file = tmpfile.name

    config = ExportJobConfig(
        db_url=db_url,
        query="SELECT * FROM sample_table",
        output_file=output_file,
        format="parquet",
    )

    path = run_export(config)
    assert os.path.exists(path)
    os.remove(path)

def test_run_export_partitioned(sample_sqlite_db):
    db_url, _ = sample_sqlite_db
    with tempfile.TemporaryDirectory() as tmpdir:
        config = ExportJobConfig(
            db_url=db_url,
            query="SELECT * FROM sample_table",
            output_dir=tmpdir,
            format="parquet",
            partition_by=["category"]
        )

        path = run_export(config)
        assert os.path.exists(path)
        found = any(
            fname.endswith(".parquet")
            for root, _, files in os.walk(path)
            for fname in files
        )
        assert found, f"No .parquet file found in {path}"


def test_run_export_parquet(tmp_path):
    config = ExportJobConfig(
        db_url="sqlite://",
        query="SELECT 42 AS answer",
        output_file=str(tmp_path / "test.parquet"),
        format="parquet"
    )
    out = run_export(config)
    assert out.endswith(".parquet")
