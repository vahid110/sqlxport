# tests/unit/test_parquet_writer.py

import unittest
import pyarrow.parquet as pq
import os
import shutil
import pandas as pd
from sqlxport.formats.parquet_writer import ParquetWriter


def test_write_single_file(tmp_path):
    df = pd.DataFrame({
        "id": [1, 2, 3],
        "name": ["Alice", "Bob", "Charlie"]
    })
    output_file = tmp_path / "output.parquet"
    
    writer = ParquetWriter()
    writer.write(df, str(output_file))

    assert output_file.exists()

    # Verify contents
    df_read = pd.read_parquet(output_file)
    pd.testing.assert_frame_equal(df, df_read)


def test_write_partitioned(tmp_path):
    df = pd.DataFrame({
        "id": [1, 2, 3],
        "category": ["A", "B", "A"]
    })
    output_dir = tmp_path / "partitioned"
    writer = ParquetWriter()
    writer.write_partitioned(df, str(output_dir), partition_by="category")

    # Expected partition folders: category=A and category=B
    expected_dirs = [output_dir / "category=A", output_dir / "category=B"]
    for d in expected_dirs:
        assert d.exists()
        files = list(d.glob("*.parquet"))
        assert len(files) == 1  # one file per partition

        df_read = pd.read_parquet(files[0])
        expected_df = df[df["category"] == d.name.split("=")[1]].reset_index(drop=True)
        expected_df = expected_df.drop(columns=["category"])  # partition column excluded from file
        df_read = df_read.reset_index(drop=True)
        pd.testing.assert_frame_equal(df_read, expected_df)

class TestParquetWriterFileCreation(unittest.TestCase):
    def setUp(self):
        self.df = pd.DataFrame({
            "id": [1],
            "msg": ["hello"],
            "group": ["a"]
        })
        self.output_file = "test_output.parquet"
        self.partition_dir = "test_exports"

    def tearDown(self):
        if os.path.exists(self.output_file):
            os.remove(self.output_file)
        if os.path.exists(self.partition_dir):
            shutil.rmtree(self.partition_dir)

    def test_write_single_file(self):
        writer = ParquetWriter()
        writer.write(self.df, self.output_file)
        self.assertTrue(os.path.exists(self.output_file))
        table = pq.read_table(self.output_file)
        self.assertEqual(table.num_columns, 3)

    def test_write_partitioned_files(self):
        writer = ParquetWriter()
        writer.write_partitioned(self.df, self.partition_dir, partition_by="group")
        partition_path = os.path.join(self.partition_dir, "group=a")
        self.assertTrue(os.path.exists(partition_path))

        # Look for a .parquet file inside the partition
        files = [f for f in os.listdir(partition_path) if f.endswith(".parquet")]
        self.assertTrue(files, f"No .parquet files found in {partition_path}")

        # Optionally read it to ensure it's valid
        full_path = os.path.join(partition_path, files[0])
        table = pq.read_table(full_path)
        self.assertGreaterEqual(table.num_columns, 1)
