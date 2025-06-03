from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="sql2data",
    version="0.1.3",
    author="Vahid Saber",
    author_email="vahid.saber78@gmail.com",
    description="Export SQL query results to Parquet, CSV, and more. Upload to S3 or MinIO.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/vahid110/sql2data",
    project_urls={
        "Bug Tracker": "https://github.com/vahid110/sql2data/issues",
    },
    classifiers=[
        "Development Status :: 4 - Beta",
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Intended Audience :: Developers",
        "Topic :: Database",
        "Topic :: Utilities",
    ],
    packages=find_packages(),
    python_requires=">=3.8",
    install_requires=[
        "click",
        "psycopg[binary]",
        "pandas",
        "pyarrow",
        "boto3",
        "python-dotenv",
        "tabulate"
    ],
    extras_require={
        "dev": [
            "pytest",
            "coverage",
            "duckdb"
        ]
    },
    entry_points={
        "console_scripts": ["sql2data=sql2data.cli.main:cli"],
    },
)
