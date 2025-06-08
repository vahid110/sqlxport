# tests/unit/test_preview_local_parquet.py
import os
import tempfile
import unittest
import pandas as pd
from sqlxport.core.storage import preview_local_parquet

class TestPreviewLocalParquet(unittest.TestCase):

    def test_preview_local_parquet_prints_head(self):
        # Create a small Parquet file
        df = pd.DataFrame({
            "name": ["Alice", "Bob"],
            "age": [30, 25]
        })

        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmpfile:
            path = tmpfile.name
        df.to_parquet(path)

        try:
            # Should not raise
            preview_local_parquet(path, max_rows=1)
        finally:
            os.remove(path)
