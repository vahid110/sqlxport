from enum import Enum

class FileQueryEngine(str, Enum):
    DUCKDB = "duckdb"
    ATHENA = "athena"
    TRINO = "trino"
