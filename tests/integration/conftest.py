# tests/integration/conftest.py

import subprocess
import pytest
import os
import time
import boto3
import uuid
from sqlxport.utils.env import load_env_file
import pyarrow as pa
import pyarrow.parquet as pq
import tempfile
from botocore.exceptions import ClientError

DOCKER_COMPOSE_FILE = os.path.join(os.path.dirname(__file__), "docker-compose.yml")
SERVICE_NAME = "sqlxport_test_pg"  # Matches docker-compose.yml container_name
WAIT_TIMEOUT = 30

def is_container_running(name):
    result = subprocess.run(["docker", "ps", "--filter", f"name={name}", "--format", "{{.Status}}"],
                            capture_output=True, text=True)
    return "Up" in result.stdout

def wait_for_postgres(host="localhost", port=5433, timeout=WAIT_TIMEOUT):
    import socket
    print(f"⏳ Waiting for PostgreSQL on {host}:{port}...")
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with socket.create_connection((host, port), timeout=1):
                print("✅ PostgreSQL is ready.")
                return True
        except OSError:
            time.sleep(1)
    raise TimeoutError(f"PostgreSQL not reachable at {host}:{port} after {timeout} seconds.")

@pytest.fixture(scope="session", autouse=True)
def manage_docker():
    container_started = False

    if not is_container_running(SERVICE_NAME):
        print("🚀 Starting Docker Compose for integration tests...")
        subprocess.run(["docker", "compose", "-f", DOCKER_COMPOSE_FILE, "up", "-d"], check=True)
        container_started = True
    else:
        print("ℹ️ Docker container already running — skipping startup.")

    wait_for_postgres()

    yield  # Tests run here

    if container_started:
        print("🧼 Tearing down Docker containers started by test...")
        subprocess.run(["docker", "compose", "-f", DOCKER_COMPOSE_FILE, "down", "-v"], check=True)
    else:
        print("ℹ️ Skipping teardown — container was already running.")

ATHENA_REGION = "us-east-1"
ATHENA_OUTPUT = "s3://vahidpersonal-east1/query-results/"

def athena_client(region=ATHENA_REGION):
    return boto3.client("athena", region_name=region)

def run_athena_query(query, database=None, region=ATHENA_REGION, output=ATHENA_OUTPUT):
    client = athena_client(region)
    kwargs = {
        "QueryString": query,
        "ResultConfiguration": {"OutputLocation": output}
    }
    if database:
        kwargs["QueryExecutionContext"] = {"Database": database}
    response = client.start_query_execution(**kwargs)
    execution_id = response["QueryExecutionId"]
    for _ in range(20):
        status = client.get_query_execution(QueryExecutionId=execution_id)["QueryExecution"]["Status"]["State"]
        if status == "SUCCEEDED":
            return execution_id
        elif status in {"FAILED", "CANCELLED"}:
            raise RuntimeError(f"Athena query failed during conftest's run_athena_query: {query}")
        time.sleep(2)
    raise TimeoutError(f"Athena query timeout: {query}")


@pytest.fixture(scope="function")
def temp_athena_database():
    db_name = f"testdb_{uuid.uuid4().hex[:8]}"
    print(f"📁 Creating temporary Athena database: {db_name}")
    run_athena_query(f"CREATE DATABASE {db_name}")
    yield db_name
    print(f"🧹 Dropping temporary Athena database: {db_name}")
    run_athena_query(f"DROP DATABASE {db_name} CASCADE")


@pytest.fixture(scope="session", autouse=True)
def minio_test_data():
    bucket = "vahid-signing"
    key = "test-data/sample.parquet"
    endpoint_url = "http://localhost:9000"
    region = "us-east-1"
    access_key = "minioadmin"
    secret_key = "minioadmin"

    print(f"📦 Ensuring MinIO bucket '{bucket}' and test data exist...")

    s3 = boto3.client("s3",
                      aws_access_key_id=access_key,
                      aws_secret_access_key=secret_key,
                      region_name=region,
                      endpoint_url=endpoint_url)

    # Create the bucket if it doesn't exist
    try:
        s3.head_bucket(Bucket=bucket)
        print(f"✅ Bucket '{bucket}' already exists.")
    except ClientError:
        print(f"📁 Creating bucket '{bucket}'...")
        s3.create_bucket(Bucket=bucket)

    # Upload a simple Parquet file
    schema = pa.schema([("id", pa.int32()), ("name", pa.string())])
    table = pa.table({"id": [1, 2], "name": ["Alice", "Bob"]}, schema=schema)

    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp_file:
        pq.write_table(table, tmp_file.name)
        tmp_file.flush()
        s3.upload_file(tmp_file.name, bucket, key)
        print(f"✅ Uploaded test Parquet file to s3://{bucket}/{key}")
