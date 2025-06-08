import boto3
import time
from .base import QueryEngine

class AthenaEngine(QueryEngine):
    def validate_table(self, table_name: str, database: str = "default", workgroup: str = "primary", **kwargs):
        query = f"SELECT COUNT(*) FROM {table_name}"
        print(f"🔍 Validating Athena table: {query}")

        athena = boto3.client("athena", region_name=kwargs.get("region", "us-east-1"))
        result = athena.start_query_execution(
            QueryString=query,
            QueryExecutionContext={"Database": database},
            WorkGroup=workgroup,
            ResultConfiguration={"OutputLocation": kwargs.get("output_location", "s3://my-athena-results/")}
        )

        exec_id = result["QueryExecutionId"]
        print(f"🔄 Athena QueryExecutionId: {exec_id}")

        # Wait for completion (simple polling)
        while True:
            status = athena.get_query_execution(QueryExecutionId=exec_id)["QueryExecution"]["Status"]["State"]
            if status in {"SUCCEEDED", "FAILED", "CANCELLED"}:
                break
            time.sleep(1)

        if status != "SUCCEEDED":
            raise RuntimeError(f"Athena query failed: {status}")
        print("✅ Athena table is queryable.")
    def set_output_location(self, output_location: str):
        self.athena_output = output_location

