# tests/unit/test_csv_writer.py

import os
import shutil
import pandas as pd
import unittest
from sqlxport.formats.csv_writer import CsvWriter


class TestCsvWriter(unittest.TestCase):
    def setUp(self):
        self.df = pd.DataFrame({
            "id": [1, 2, 3],
            "category": ["A", "B", "A"]
        })
        self.output_file = "test_output.csv"
        self.partition_dir = "test_csv_exports"

    def tearDown(self):
        if os.path.exists(self.output_file):
            os.remove(self.output_file)
        if os.path.exists(self.partition_dir):
            shutil.rmtree(self.partition_dir)

    def test_write_single_csv_file(self):
        writer = CsvWriter()
        writer.write(self.df, self.output_file)
        self.assertTrue(os.path.exists(self.output_file))

        df_read = pd.read_csv(self.output_file)
        pd.testing.assert_frame_equal(self.df, df_read)

    def test_write_partitioned_csv_files(self):
        writer = CsvWriter()
        writer.write_partitioned(self.df, self.partition_dir, partition_by="category")

        for cat in ["A", "B"]:
            path = os.path.join(self.partition_dir, f"category={cat}", "part-0000.csv")
            self.assertTrue(os.path.exists(path))
            df_read = pd.read_csv(path)
            expected = self.df[self.df["category"] == cat].drop(columns=["category"]).reset_index(drop=True)
            df_read = df_read.reset_index(drop=True)
            pd.testing.assert_frame_equal(df_read, expected)

    def test_write_flat_csv_file(self):
        writer = CsvWriter()
        output_dir = "test_flat_dir"
        os.makedirs(output_dir, exist_ok=True)

        try:
            writer.write_flat(self.df, output_dir)
            output_path = os.path.join(output_dir, "output.csv")
            self.assertTrue(os.path.exists(output_path))

            df_read = pd.read_csv(output_path)
            pd.testing.assert_frame_equal(self.df, df_read)
        finally:
            shutil.rmtree(output_dir)
