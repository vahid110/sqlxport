# sqlxport/formats/csv_writer.py

import os
import pandas as pd
from .base import FormatWriter

class CsvWriter(FormatWriter):
    def __call__(self, df, output_file=None, output_dir=None, partition_by=None):
        if partition_by:
            self.write_partitioned(df, output_dir, partition_by[0] if isinstance(partition_by, list) else partition_by)
            return output_dir
        elif output_dir:
            self.write_flat(df, output_dir)
            return os.path.join(output_dir, "output.csv")
        elif output_file:
            self.write_to_file(df, output_file)
            return output_file
        else:
            raise ValueError("Either output_file or output_dir must be specified.")

    def write_to_file(self, df: pd.DataFrame, output_file: str):
        df.to_csv(output_file, index=False)
        print(f"✅ CSV file saved to {output_file}")

    def write(self, df: pd.DataFrame, config):
        self.write_to_file(df, config.output_file)

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
