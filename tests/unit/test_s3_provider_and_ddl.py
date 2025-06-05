import os
import tempfile
import pandas as pd
import unittest
from sqlxport.ddl import utils
from sqlxport.cli.main import warn_if_s3_endpoint_suspicious

class TestGenerateAthenaDDL(unittest.TestCase):

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.df = pd.DataFrame({
            "id": [1, 2, 3],
            "region": ["EMEA", "NA", "APAC"],
            "amount": [100.0, 200.0, 150.0]
        })
        self.file_path = os.path.join(self.temp_dir.name, "test.parquet")
        self.df.to_parquet(self.file_path, index=False)

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_generate_athena_ddl_from_file(self):
        ddl = utils.generate_athena_ddl(self.file_path, "s3://bucket/path", "test_table", partition_cols=["region"])
        self.assertIn("CREATE EXTERNAL TABLE IF NOT EXISTS test_table", ddl)
        self.assertIn("region STRING", ddl)

    def test_generate_athena_ddl_from_partitioned_dir(self):
        partitioned_dir = os.path.join(self.temp_dir.name, "region=EMEA")
        os.makedirs(partitioned_dir)
        part_file = os.path.join(partitioned_dir, "part-0000.parquet")
        self.df.to_parquet(part_file, index=False)

        ddl = utils.generate_athena_ddl(self.temp_dir.name, "s3://bucket/path", "test_table", partition_cols=["region"])
        self.assertIn("CREATE EXTERNAL TABLE IF NOT EXISTS test_table", ddl)
        self.assertIn("region STRING", ddl)


class TestS3ProviderWarning(unittest.TestCase):

    def test_warn_if_s3_endpoint_suspicious_custom(self):
        from io import StringIO
        import sys

        captured_output = StringIO()
        sys.stdout = captured_output
        warn_if_s3_endpoint_suspicious("", "minio")
        sys.stdout = sys.__stdout__

        self.assertIn("Warning: No --s3-endpoint specified while using 'minio'", captured_output.getvalue())


    def test_warn_if_s3_endpoint_suspicious_amazon(self):
        from io import StringIO
        import sys

        captured_output = StringIO()
        sys.stdout = captured_output
        warn_if_s3_endpoint_suspicious("https://s3.amazonaws.com", "aws")
        sys.stdout = sys.__stdout__

        self.assertNotIn("Warning", captured_output.getvalue())
