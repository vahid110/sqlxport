import pandas as pd
from sqlalchemy import create_engine, text

def fetch_query_as_dataframe(db_url: str, query: str) -> pd.DataFrame:
    engine = create_engine(db_url)
    with engine.connect() as conn:
        return pd.read_sql_query(query, conn)
