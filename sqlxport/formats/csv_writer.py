# sqlxport/formats/csv_writer.py

import os
import pandas as pd
from .base import FormatWriter

class CsvWriter(FormatWriter):
    def write(self, df: pd.DataFrame, output_file: str):
        df.to_csv(output_file, index=False)

    def write_partitioned(self, df: pd.DataFrame, output_dir: str, partition_by: str):
        if partition_by not in df.columns:
            raise ValueError(f"Partition column '{partition_by}' not found in DataFrame")

        os.makedirs(output_dir, exist_ok=True)

        for value, group in df.groupby(partition_by):
            partition_path = os.path.join(output_dir, f"{partition_by}={value}")
            os.makedirs(partition_path, exist_ok=True)
            file_path = os.path.join(partition_path, "part-0000.csv")
            group.drop(columns=[partition_by]).to_csv(file_path, index=False)

    def write_flat(self, df: pd.DataFrame, output_dir: str):
        os.makedirs(output_dir, exist_ok=True)
        file_path = os.path.join(output_dir, "output.csv")
        df.to_csv(file_path, index=False)
