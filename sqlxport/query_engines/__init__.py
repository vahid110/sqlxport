from sqlxport.enums import FileQueryEngine
from .duckdb_engine import DuckDBEngine
from .athena_engine import AthenaEngine
from .base import QueryEngine

def get_query_engine(engine_name: str) -> QueryEngine:
    engine_name = engine_name.lower()
    if engine_name == FileQueryEngine.DUCKDB:
        return DuckDBEngine()
    if engine_name == FileQueryEngine.ATHENA:
        return AthenaEngine()
    raise ValueError(f"Unsupported file query engine: {engine_name}")
