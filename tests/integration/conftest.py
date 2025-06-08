import subprocess
import pytest
import os
import time

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
