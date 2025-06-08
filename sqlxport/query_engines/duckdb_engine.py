import duckdb
from .base import QueryEngine

class DuckDBEngine(QueryEngine):
    def preview(self, file_path: str, limit: int = 10) -> str:
        con = duckdb.connect()
        df = con.sql(f"SELECT * FROM '{file_path}' LIMIT {limit}").df()
        return df.to_string(index=False)

    def infer_schema(self, file_path: str):
        con = duckdb.connect()
        df = con.sql(f"DESCRIBE SELECT * FROM '{file_path}'").df()
        return [
            {"name": row["column_name"], "type": row["column_type"]}
            for _, row in df.iterrows()
        ]

    def validate_table(self, table_name: str, **kwargs):
        print(f"✅ Validated via DuckDB (placeholder)")
