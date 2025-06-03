import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from sqlalchemy import create_engine

engine = create_engine("postgresql://testuser:testpass@localhost:5432/testdb")
df = pd.read_sql("SELECT * FROM users", engine)

table = pa.Table.from_pandas(df)
pq.write_table(table, "users.parquet")

print("✅ Manual export complete.")