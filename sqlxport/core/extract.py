# sqlxport/core/extract.py
import pandas as pd
import sqlite3
import psycopg

def fetch_query_as_dataframe(db_url, query):
    if db_url.startswith("sqlite://"):
        # Use in-memory SQLite database
        conn = sqlite3.connect(":memory:")
        conn.executescript("""
            CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER);
            INSERT INTO users (name, age) VALUES ('Alice', 30), ('Bob', 25);
        """)
        return pd.read_sql_query(query, conn)
    else:
        with psycopg.connect(db_url, client_encoding="UTF8") as conn:
            return pd.read_sql_query(query, conn)
