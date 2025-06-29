from sqlxport.utils.summary import summarize_file
import os
import pandas as pd

# ✅ Ensure testdata directory exists
os.makedirs("tests/integration/testdata", exist_ok=True)

# ✅ Generate test Parquet file
df = pd.DataFrame({
    "user_id": [1, 2, 3],
    "status": ["active", "inactive", "active"],
    "created_at": pd.to_datetime(["2023-01-01", "2023-06-01", "2023-07-01"]),
    "email": ["a@x.com", None, "b@y.com"],
    "age": [30, 45, 50],
})
df.to_parquet("tests/integration/testdata/exported_sample.parquet")

def test_summarize_parquet_file():
    path = "tests/integration/testdata/exported_sample.parquet"
    summary = summarize_file(path)
    assert "rows and" in summary
    assert "`" in summary
    assert "unique values" in summary or "range" in summary
