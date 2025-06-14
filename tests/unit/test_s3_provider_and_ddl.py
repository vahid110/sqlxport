# tests/unit/test_s3_provider_and_ddl.py

import os
import sys
import tempfile
import pandas as pd
import unittest
from io import StringIO

from sqlxport.ddl import utils
from sqlxport.cli.utils import warn_if_s3_endpoint_suspicious


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
        ddl_lower = ddl.lower()

        # Ensure "region" appears only once in the DDL — in the PARTITIONED BY clause
        region_mentions = [line for line in ddl_lower.splitlines() if "region" in line]
        self.assertEqual(len(region_mentions), 1, f"'region' appears too many times: {region_mentions}")

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
        captured_output = StringIO()
        sys.stdout = captured_output
        try:
            warn_if_s3_endpoint_suspicious("", "minio")
        finally:
            sys.stdout = sys.__stdout__

        self.assertIn("Warning: No --s3-endpoint specified while using 'minio'", captured_output.getvalue())

    def test_warn_if_s3_endpoint_suspicious_amazon(self):
        captured_output = StringIO()
        sys.stdout = captured_output
        try:
            warn_if_s3_endpoint_suspicious("https://s3.amazonaws.com", "aws")
        finally:
            sys.stdout = sys.__stdout__

        self.assertNotIn("Warning", captured_output.getvalue())
