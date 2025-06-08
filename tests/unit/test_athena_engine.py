import pytest
from sqlxport.query_engines.athena_engine import AthenaEngine

@pytest.mark.skip(reason="Integration with live AWS Athena. Requires real AWS setup.")
def test_athena_validate_table_live():
    engine = AthenaEngine()
    engine.validate_table(
        table_name="your_table",
        database="your_database",
        region="us-east-1",
        output_location="s3://your-athena-results/"
    )
