import boto3
import time

def start_athena_query(region, query, database, output):
    client = boto3.client("athena", region_name=region)
    response = client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Database": database},
        ResultConfiguration={"OutputLocation": output},
    )
    return response["QueryExecutionId"]

def wait_for_query_success(client, execution_id):
    for _ in range(20):
        state = client.get_query_execution(QueryExecutionId=execution_id)["QueryExecution"]["Status"]["State"]
        if state == "SUCCEEDED":
            return True
        elif state in ("FAILED", "CANCELLED"):
            return False
        time.sleep(2)
    return False

def register_table_in_glue(region, ddl_path, database, output):
    with open(ddl_path) as f:
        query = f.read()
    execution_id = start_athena_query(region, query, database, output)
    client = boto3.client("athena", region_name=region)
    if wait_for_query_success(client, execution_id):
        print(f"✅ Glue table registered: {execution_id}")
    else:
        raise RuntimeError("❌ Glue table registration failed")

def repair_partitions_in_glue(region, table_name, database, output):
    query = f"MSCK REPAIR TABLE {table_name};"
    execution_id = start_athena_query(region, query, database, output)
    client = boto3.client("athena", region_name=region)
    if wait_for_query_success(client, execution_id):
        print("✅ Partition repair completed")
    else:
        raise RuntimeError("❌ Partition repair failed")

def validate_glue_table(region, table_name, database, output):
    query = f"SELECT service, COUNT(*) FROM {table_name} GROUP BY service;"
    execution_id = start_athena_query(region, query, database, output)
    client = boto3.client("athena", region_name=region)
    if wait_for_query_success(client, execution_id):
        print("✅ Validation query succeeded")
    else:
        raise RuntimeError("❌ Validation query failed")