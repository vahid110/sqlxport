# sqlxport/cli/glue_ops.py

import boto3
import time
import botocore.exceptions


def start_athena_query(region, query, database, output):
    client = boto3.client("athena", region_name=region)
    try:
        response = client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={"Database": database},
            ResultConfiguration={"OutputLocation": output},
        )
        execution_id = response["QueryExecutionId"]
        print(f"🚀 Athena query submitted. Execution ID: {execution_id}")
        return execution_id
    except botocore.exceptions.ClientError as e:
        print("❌ Failed to start Athena query execution:")
        print(e.response["Error"]["Message"])
        raise


def wait_for_query_success(client, execution_id):
    for _ in range(20):
        status = client.get_query_execution(QueryExecutionId=execution_id)["QueryExecution"]["Status"]
        state = status["State"]
        print(f"   → Athena query state: {state}")
        if state == "SUCCEEDED":
            return True
        elif state in ("FAILED", "CANCELLED"):
            reason = status.get("StateChangeReason", "No reason provided.")
            print(f"❌ Query failed: {reason}")
            return False
        time.sleep(2)
    print("❌ Query timed out after waiting.")
    return False


def register_table_in_glue(region, ddl_path, database, output):
    print(f"📄 Reading DDL from: {ddl_path}")
    with open(ddl_path) as f:
        query = f.read()

    print("📤 Submitting Glue table DDL to Athena:")
    print(query)

    try:
        execution_id = start_athena_query(region, query, database, output)
        client = boto3.client("athena", region_name=region)
        if wait_for_query_success(client, execution_id):
            print(f"✅ Glue table registered: {execution_id}")
        else:
            raise RuntimeError("❌ Glue table registration failed")
    except Exception as e:
        print("❌ Exception during Glue table registration:")
        print(str(e))
        raise


def repair_partitions_in_glue(region, table_name, database, output):
    query = f"MSCK REPAIR TABLE {table_name};"
    print("🛠  Running MSCK REPAIR TABLE...")
    print(query)
    execution_id = start_athena_query(region, query, database, output)
    client = boto3.client("athena", region_name=region)
    if wait_for_query_success(client, execution_id):
        print(f"✅ Partition repair completed. Execution ID: {execution_id}")
    else:
        raise RuntimeError("❌ Partition repair failed")


def validate_glue_table(region, table_name, database, output):
    query = f"SELECT service, COUNT(*) FROM {table_name} GROUP BY service;"
    print("🔎 Validating Glue table with Athena query:")
    print(query)
    execution_id = start_athena_query(region, query, database, output)
    client = boto3.client("athena", region_name=region)
    if wait_for_query_success(client, execution_id):
        print(f"✅ Validation query succeeded. Execution ID: {execution_id}")
    else:
        raise RuntimeError("❌ Validation query failed")
