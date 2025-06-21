# tests/integration/conftest.py

import subprocess
import pytest
import os
import time
import boto3
import uuid
from sqlxport.utils.env import load_env_file

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
            raise RuntimeError(f"Athena query failed: {query}")
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
