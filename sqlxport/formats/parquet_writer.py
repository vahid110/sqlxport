# sqlxport/formats/parquet_writer.py
import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from sqlxport.formats.base import FormatWriter

class ParquetWriter(FormatWriter):
    def write(self, df: pd.DataFrame, file_path: str) -> None:
        table = pa.Table.from_pandas(df)
        pq.write_table(table, file_path)
        print(f"✅ Parquet file saved to {file_path}")

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

    def write_flat(self, df: pd.DataFrame, output_dir: str):
        os.makedirs(output_dir, exist_ok=True)
        output_path = os.path.join(output_dir, "output.parquet")
        table = pa.Table.from_pandas(df)
        pq.write_table(table, output_path)
