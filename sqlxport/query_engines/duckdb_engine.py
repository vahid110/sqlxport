# sqlxport/query_engines/duckdb_engine.py

import duckdb
from .base import QueryEngine


import os
import duckdb

def load_schema_duckdb(file_path: str):
    """
    Infers the schema of a Parquet or CSV file or directory using DuckDB.
    Returns a list of dicts with 'name' and 'type' keys.
    """
    con = duckdb.connect()
    try:
        if os.path.isdir(file_path):
            glob_pattern = os.path.join(file_path, "*.parquet")
            query = f"DESCRIBE SELECT * FROM read_parquet('{glob_pattern}')"
        else:
            query = f"DESCRIBE SELECT * FROM '{file_path}'"
        
        df = con.sql(query).df()
        return [
            {"name": row["column_name"], "type": row["column_type"]}
            for _, row in df.iterrows()
        ]
    finally:
        con.close()


class DuckDBEngine(QueryEngine):
    def preview(self, file_path: str, limit: int = 10) -> str:
        con = duckdb.connect()
        try:
            df = con.sql(f"SELECT * FROM '{file_path}' LIMIT {limit}").df()
            return df.to_string(index=False)
        finally:
            con.close()

    def infer_schema(self, file_path: str):
        return load_schema_duckdb(file_path)

    def validate_table(self, table_name: str, **kwargs):
        print(f"✅ Validated via DuckDB (placeholder)")
