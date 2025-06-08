# sqlxport/formats/parquet_writer.py

import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from sqlxport.formats.base import FormatWriter

class ParquetWriter(FormatWriter):
    def __call__(self, df, output_file=None, output_dir=None, partition_by=None):
        if partition_by:
            self.write_partitioned(df, output_dir, partition_by[0] if isinstance(partition_by, list) else partition_by)
            return output_dir
        elif output_dir:
            return self.write_flat(df, output_dir)
        elif output_file:
            return self.write(df, output_file)
        else:
            raise ValueError("Either output_file or output_dir must be specified.")

    def write(self, df: pd.DataFrame, output_file: str) -> str:
        table = pa.Table.from_pandas(df)
        pq.write_table(table, output_file)
        print(f"✅ Parquet file saved to {output_file}")
        return output_file

    def write_partitioned(self, df: pd.DataFrame, output_dir: str, partition_by: str = None) -> None:
        table = pa.Table.from_pandas(df)
        if partition_by and partition_by in df.columns:
            pq.write_to_dataset(
                table,
                root_path=output_dir,
                partition_cols=[partition_by]
            )
        else:
            os.makedirs(output_dir, exist_ok=True)
            pq.write_table(table, os.path.join(output_dir, "part-0000.parquet"))

    def write_flat(self, df: pd.DataFrame, output_dir: str) -> str:
        os.makedirs(output_dir, exist_ok=True)
        output_path = os.path.join(output_dir, "output.parquet")
        table = pa.Table.from_pandas(df)
        pq.write_table(table, output_path)
        print(f"✅ Parquet file saved to {output_path}")
        return output_path
