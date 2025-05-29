# setup.py
from setuptools import setup, find_packages

setup(
    name="sql2data",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "click",
        "psycopg[binary]",
        "pandas",
        "pyarrow",
        "boto3",
        "python-dotenv",
    ],
    entry_points={"console_scripts": ["sql2data=sql2data.cli.main:cli"]},
    author="Vahid Saber",
    description="Export SQL query results to Parquet and upload to S3 or MinIO",
    python_requires=">=3.8",
)
