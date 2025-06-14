# sqlxport/cli/cmd_generate_env.py

import click

@click.command("generate-env")
def generate_env():
    """Generate a sample .env.example file with placeholder config."""
    example = """\
DB_URL=postgresql://username:password@localhost:5432/dbname
S3_BUCKET=data-exports
S3_KEY=users.parquet
S3_ENDPOINT=http://localhost:9000
S3_ACCESS_KEY=minioadmin
S3_SECRET_KEY=minioadmin
IAM_ROLE=arn:aws:iam::123456789012:role/MyUnloadRole
S3_OUTPUT_PREFIX=s3://data-exports/unload/
"""
    with open(".env.example", "w") as f:
        f.write(example)

    print("✅ .env.example template generated.")
