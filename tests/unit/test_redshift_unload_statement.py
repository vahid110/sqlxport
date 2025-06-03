# tests/unit/test_redshift_unload_statement.py

import textwrap
import pytest
from sqlxport.redshift_unload import generate_unload_statement

def test_generate_unload_parquet_default():
    sql = generate_unload_statement(
        query="SELECT * FROM users",
        s3_path="s3://my-bucket/export/",
        iam_role="arn:aws:iam::123456789012:role/MyUnloadRole"
    )
    expected = textwrap.dedent("""\
        UNLOAD ('SELECT * FROM users') TO 's3://my-bucket/export/' IAM_ROLE 'arn:aws:iam::123456789012:role/MyUnloadRole' FORMAT AS PARQUET ;
    """).strip()
    assert sql.strip() == expected

def test_generate_unload_csv_with_options():
    sql = generate_unload_statement(
        query="SELECT id, name FROM users",
        s3_path="s3://data/csv/",
        iam_role="arn:aws:iam::999999999999:role/CsvRole",
        format="csv",
        options=["ALLOWOVERWRITE", "DELIMITER ','"]
    )
    expected = textwrap.dedent("""\
        UNLOAD ('SELECT id, name FROM users') TO 's3://data/csv/' IAM_ROLE 'arn:aws:iam::999999999999:role/CsvRole' FORMAT AS CSV ALLOWOVERWRITE DELIMITER ',';
    """).strip()
    assert sql.strip() == expected

def test_generate_unload_unsupported_format():
    with pytest.raises(ValueError, match="Unsupported format: xml"):
        generate_unload_statement(
            query="SELECT * FROM table",
            s3_path="s3://bucket/",
            iam_role="arn:aws:iam::role/TestRole",
            format="xml"
        )
