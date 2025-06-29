# sqlxport/query_engines/athena_engine.py

import boto3
import time
import os
import uuid
import pandas as pd
from sqlxport.ddl.utils import generate_athena_ddl
from sqlxport.query_engines.duckdb_engine import load_schema_duckdb
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
            raise RuntimeError(f"Athena query failed during validate_table: {status}")
        print("✅ Athena table is queryable.")

    def set_output_location(self, output_location: str):
        self.athena_output = output_location

    def preview(self, file_path: str, limit: int = 10, **kwargs) -> str:
        import pandas as pd
        import os
        import uuid
        import time
        import boto3

        from sqlxport.query_engines.duckdb_engine import load_schema_duckdb
        from sqlxport.ddl.utils import generate_athena_ddl_parquet, generate_athena_ddl_csv

        database = kwargs.get("database")
        region = kwargs.get("region") or os.getenv("AWS_REGION")
        output_location = kwargs.get("output_location") or os.getenv("ATHENA_OUTPUT")
        workgroup = kwargs.get("workgroup", "primary")
        s3_path = kwargs.get("s3_path")
        file_format = kwargs.get("file_format", "parquet")

        partition_cols_raw = kwargs.get("partition_cols")
        partition_cols = (
            [c.strip() for c in partition_cols_raw.split(",")]
            if isinstance(partition_cols_raw, str)
            else partition_cols_raw
        )

        if not database:
            raise ValueError("Missing required engine arg: 'database'")
        if not region:
            raise ValueError("Missing required engine arg or env var: 'region' or AWS_REGION")
        if not output_location:
            raise ValueError("Missing required engine arg or env var: 'output_location' or ATHENA_OUTPUT'")
        if not s3_path:
            raise ValueError("Missing required engine arg: 's3_path' (S3 LOCATION for Athena table)")

        schema_df = load_schema_duckdb(file_path)

        table_name = f"preview_{uuid.uuid4().hex[:8]}"
        print(f"🔧 Creating temporary Athena table: {database}.{table_name}")
        print(f"📦 Target S3 path: {s3_path}")
        if file_format == "parquet":
            ddl = generate_athena_ddl_parquet(
                local_parquet_path=file_path,
                table_name=f"{database}.{table_name}",
                s3_prefix=s3_path,
                schema_df=schema_df,
                partition_cols=partition_cols
            )
        elif file_format == "csv":
            ddl = generate_athena_ddl_csv(
                local_parquet_path=file_path,
                table_name=f"{database}.{table_name}",
                s3_prefix=s3_path,
                schema_df=schema_df,
                partition_cols=partition_cols
            )
        else:
            raise ValueError(f"Unsupported file_format: {file_format}")


        print("📄 Table DDL:\n", ddl.strip())

        athena = boto3.client("athena", region_name=region)

        athena.start_query_execution(
            QueryString=ddl,
            QueryExecutionContext={"Database": database},
            WorkGroup=workgroup,
            ResultConfiguration={"OutputLocation": output_location}
        )

        time.sleep(2)

        preview_query = f"SELECT * FROM {database}.{table_name} LIMIT {limit}"
        print("🔍 Preview query:\n", preview_query)

        result = athena.start_query_execution(
            QueryString=preview_query,
            QueryExecutionContext={"Database": database},
            WorkGroup=workgroup,
            ResultConfiguration={"OutputLocation": output_location}
        )
        exec_id = result["QueryExecutionId"]
        print(f"📜 QueryExecutionId: {exec_id}")

        for _ in range(30):
            response = athena.get_query_execution(QueryExecutionId=exec_id)
            status = response["QueryExecution"]["Status"]["State"]
            if status in {"SUCCEEDED", "FAILED", "CANCELLED"}:
                break
            time.sleep(1)

        if status != "SUCCEEDED":
            reason = response["QueryExecution"]["Status"].get("StateChangeReason", "Unknown reason")
            print(f"❌ Athena query failed: {status} — {reason}")
            raise RuntimeError(f"Athena query failed during preview: {status} — {reason}")

        output = athena.get_query_results(QueryExecutionId=exec_id)
        columns = [col["Label"] for col in output["ResultSet"]["ResultSetMetadata"]["ColumnInfo"]]
        rows = output["ResultSet"]["Rows"][1:]  # skip header
        data = [[f.get("VarCharValue", "") for f in r["Data"]] for r in rows]
        markdown = pd.DataFrame(data, columns=columns).to_markdown(index=False)

        print(f"🪩 Dropping temporary table: {database}.{table_name}")
        athena.start_query_execution(
            QueryString=f"DROP TABLE IF EXISTS {database}.{table_name}",
            QueryExecutionContext={"Database": database},
            WorkGroup=workgroup,
            ResultConfiguration={"OutputLocation": output_location}
        )

        return markdown

