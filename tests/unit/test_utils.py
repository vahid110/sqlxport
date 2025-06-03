# tests/unit/test_utils.py

import os
import unittest
import pyarrow as pa
import pyarrow.parquet as pq
import tempfile
from sqlxport.ddl.utils import generate_athena_ddl





def test_generate_athena_ddl_basic():
    # Create a sample DataFrame and write as parquet
    table = pa.table({
        "name": pa.array(["Alice", "Bob"]),
        "age": pa.array([30, 25], type=pa.int32())
    })

    with tempfile.NamedTemporaryFile(delete=False, suffix=".parquet") as tmp:
        pq.write_table(table, tmp.name)
        parquet_path = tmp.name

    ddl = generate_athena_ddl(
        parquet_path,
        s3_prefix="s3://my-bucket/my-prefix/",
        table_name="users"
    )

    assert "CREATE EXTERNAL TABLE users" in ddl
    assert "name string" in ddl.lower()
    assert "age int" in ddl.lower()
    assert "LOCATION 's3://my-bucket/my-prefix/'" in ddl

    os.unlink(parquet_path)  # cleanup


import unittest
import pyarrow as pa
from sqlxport.ddl.utils import arrow_to_athena_type

class TestArrowToAthenaType(unittest.TestCase):
    def test_basic_types(self):
        self.assertEqual(arrow_to_athena_type(pa.string()), "STRING")
        self.assertEqual(arrow_to_athena_type(pa.int8()), "TINYINT")
        self.assertEqual(arrow_to_athena_type(pa.int16()), "TINYINT")
        self.assertEqual(arrow_to_athena_type(pa.int32()), "INT")
        self.assertEqual(arrow_to_athena_type(pa.int64()), "BIGINT")
        self.assertEqual(arrow_to_athena_type(pa.float32()), "FLOAT")
        self.assertEqual(arrow_to_athena_type(pa.float64()), "DOUBLE")
        self.assertEqual(arrow_to_athena_type(pa.bool_()), "BOOLEAN")
        self.assertEqual(arrow_to_athena_type(pa.date32()), "DATE")
        self.assertEqual(arrow_to_athena_type(pa.timestamp('ns')), "TIMESTAMP")

    def test_decimal_type(self):
        t = pa.decimal128(12, 4)
        self.assertEqual(arrow_to_athena_type(t), "DECIMAL(12, 4)")

    def test_binary_and_map(self):
        self.assertEqual(arrow_to_athena_type(pa.binary()), "VARBINARY")
        self.assertEqual(arrow_to_athena_type(pa.map_(pa.string(), pa.string())), "MAP<STRING, STRING>")

    def test_complex_types(self):
        self.assertEqual(arrow_to_athena_type(pa.list_(pa.string())), "ARRAY<STRING>")
        struct_type = pa.struct([("a", pa.int32()), ("b", pa.string())])
        self.assertEqual(arrow_to_athena_type(struct_type), "STRUCT<a: INT, b: STRING>")

    def test_nested_struct(self):
        nested_struct = pa.struct([("a", pa.struct([("x", pa.int32())])), ("b", pa.string())])
        self.assertEqual(arrow_to_athena_type(nested_struct), "STRUCT<a: STRUCT<x: INT>, b: STRING>")

    def test_fallback(self):
        self.assertEqual(arrow_to_athena_type(pa.null()), "STRING")