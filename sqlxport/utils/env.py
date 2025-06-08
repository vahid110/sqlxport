# sqlxport/utils/env.py

import os
from dotenv import dotenv_values

def load_env_file(path: str = "tests/.env.test") -> dict:
    """Loads key-value pairs from a .env-style file into a dictionary."""
    if not os.path.exists(path):
        raise FileNotFoundError(f"Missing config file: {path}")
    return dotenv_values(path)

def load_redshift_config() -> tuple[str, str]:
    """Returns (REDSHIFT_DB_URL, REDSHIFT_IAM_ROLE)."""
    env = load_env_file()
    return env["REDSHIFT_DB_URL"], env["REDSHIFT_IAM_ROLE"]

def load_s3_config() -> dict:
    """Returns a dictionary with AWS and S3 config keys."""
    env = load_env_file()
    return {
        "S3_BUCKET": env["S3_BUCKET"],
        "S3_ENDPOINT_URL": env.get("S3_ENDPOINT_URL"),
        "AWS_REGION": env.get("AWS_REGION", "us-east-1"),
        "AWS_ACCESS_KEY_ID": env["AWS_ACCESS_KEY_ID"],
        "AWS_SECRET_ACCESS_KEY": env["AWS_SECRET_ACCESS_KEY"]
    }
