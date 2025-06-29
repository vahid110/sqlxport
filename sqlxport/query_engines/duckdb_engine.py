# sqlxport/query_engines/duckdb_engine.py

import os
import duckdb
from .base import QueryEngine
from pathlib import Path
from typing import Union


def load_schema_duckdb(file_path: str | list[str]):
    import duckdb
    import os
    from pathlib import Path

    con = duckdb.connect()
    try:
        if isinstance(file_path, list):
            paths = [str(p) for p in file_path]
            ext = Path(paths[0]).suffix.lower()
            quoted = ", ".join(f"'{p}'" for p in paths)
        else:
            ext = Path(file_path).suffix.lower()

        if ext == ".csv":
            query = f"DESCRIBE SELECT * FROM read_csv_auto('{file_path}', header=True)"
        elif ext == ".parquet":
            if isinstance(file_path, list):
                query = f"DESCRIBE SELECT * FROM read_parquet([{quoted}])"
            elif os.path.isdir(file_path):
                glob_pattern = os.path.join(file_path, "*.parquet")
                query = f"DESCRIBE SELECT * FROM read_parquet('{glob_pattern}')"
            else:
                query = f"DESCRIBE SELECT * FROM read_parquet('{file_path}')"
        else:
            raise ValueError(f"Unsupported file type for schema inference: {ext}")

        df = con.sql(query).df()
        return [{"name": row["column_name"], "type": row["column_type"]} for _, row in df.iterrows()]
    finally:
        con.close()
class DuckDBEngine(QueryEngine):
    def infer_schema(self, input_file: str) -> list[dict]:
        import os
        from sqlxport.ddl.utils import find_first_parquet, find_first_csv

        if os.path.isdir(input_file):
            # Detect file format by probing the directory
            file = find_first_parquet(input_file)
            if not file:
                file = find_first_csv(input_file)
            if not file:
                raise FileNotFoundError(f"No Parquet or CSV files found under {input_file}")
            input_file = file

        _, ext = os.path.splitext(input_file.lower())
        if ext == ".csv":
            query = f"SELECT * FROM read_csv_auto('{input_file}') LIMIT 1"
        else:
            query = f"SELECT * FROM read_parquet('{input_file}') LIMIT 1"

        table = duckdb.query(query).to_arrow_table()
        return [{"name": f.name, "type": f.type} for f in table.schema]


    def preview(self, file_path: Union[str, Path], **kwargs) -> str:
        con = duckdb.connect()
        file_path = str(file_path)

        ext = Path(file_path).suffix.lower()
        if ext == ".csv":
            query = f"SELECT * FROM read_csv_auto('{file_path}', header=True)"
        elif ext == ".parquet":
            query = f"SELECT * FROM read_parquet('{file_path}')"
        else:
            raise ValueError(f"Unsupported file type for DuckDB preview: {ext}")

        df = con.sql(query).df()
        return df.head().to_string(index=False)


