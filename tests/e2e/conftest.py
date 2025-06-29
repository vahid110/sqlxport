import pytest
import os
import pandas as pd

@pytest.fixture
def sample_parquet_file():
    path = "tests/integration/testdata/exported_sample.parquet"
    os.makedirs(os.path.dirname(path), exist_ok=True)

    df = pd.DataFrame({
        "user_id": [1, 2, 3],
        "status": ["active", "inactive", "active"],
        "created_at": pd.to_datetime(["2023-01-01", "2023-06-01", "2023-07-01"]),
        "email": ["a@x.com", None, "b@y.com"],
        "age": [30, 45, 50],
    })
    df.to_parquet(path)
    return path
