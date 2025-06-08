[![PyPI version](https://badge.fury.io/py/sqlxport.svg)](https://pypi.org/project/sqlxport/)
![CI](https://github.com/vahid110/sqlxport/actions/workflows/ci.yml/badge.svg)
[![Code Style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

<p align="center">
  <img src=".github/logo.png" alt="SQLxport Logo" width="200"/>
</p>

# sqlxport

**Modular CLI + API tool to extract data from PostgreSQL, Redshift, SQLite (and more), exporting to formats like Parquet/CSV, with optional S3 upload and Athena integration.**

---

## ✅ Features

* 🔄 Run custom SQL queries against PostgreSQL, Redshift, SQLite
* 📦 Export to Parquet or CSV (`--format`)
* 🩣 Upload results to S3 or MinIO
* 🔄 Redshift `UNLOAD` support (`--export-mode redshift-unload`)
* 🧹 Partition output by column
* 📜 Generate Athena `CREATE TABLE` DDL
* 🔍 Preview local or remote Parquet/CSV files
* ⚙️ `.env` support for convenient config
* 🐍 Reusable Python API

---

## ❓ Why SQLxport?

SQLxport simplifies data export workflows and is designed for automation:

* ✅ One command gives you SQL → Parquet/CSV → S3
* 🧱 Works locally, in CI, or inside Docker
* 🪢 Connects to Athena, MinIO, Redshift easily
* 🔌 Clean format and database plugin model
* 🧪 Fully tested, scriptable, production-ready

---

## 📦 Installation

```bash
pip install .
# or for development
pip install -e .
```

---

## 🚀 Usage

### Choose Export Mode

| `--export-mode`        | Compatible DB URLs               | Description              |
|------------------------|----------------------------------|--------------------------|
| `postgres-query`       | `postgresql://`, `postgres://`   | SELECT + local export   |
| `redshift-unload`      | `redshift://`                    | UNLOAD to S3             |
| `sqlite-query`         | `sqlite:///path.db`              | For local/lightweight testing |

---

### CLI Examples

#### Basic Export

```bash
sqlxport run \
  --export-mode postgres-query \
  --db-url postgresql://user:pass@localhost:5432/mydb \
  --query "SELECT * FROM users" \
  --output-file users.parquet \
  --format parquet
```

#### S3 Upload

```bash
sqlxport run \
  --export-mode postgres-query \
  --db-url postgresql://... \
  --query "..." \
  --output-file users.parquet \
  --s3-bucket my-bucket \
  --s3-key users.parquet \
  --s3-access-key AKIA... \
  --s3-secret-key ... \
  --s3-endpoint https://s3.amazonaws.com
```

#### Partitioned Export

```bash
sqlxport run \
  --export-mode postgres-query \
  --db-url postgresql://... \
  --query "..." \
  --output-dir output/ \
  --partition-by group_column \
  --format csv
```

#### Redshift UNLOAD Mode

```bash
sqlxport run \
  --export-mode redshift-unload \
  --db-url redshift://... \
  --query "SELECT * FROM large_table" \
  --s3-output-prefix s3://bucket/unload/ \
  --iam-role arn:aws:iam::123456789012:role/MyUnloadRole
```

---

## 🐍 Python API

```python
from sqlxport.api.export import run_export, ExportJobConfig

config = ExportJobConfig(
    db_url="sqlite:///test.db",
    query="SELECT * FROM users",
    format="csv",
    output_file="out.csv",
    export_mode="sqlite-query"
)

run_export(config)
```

---

## 🧪 Running Tests

```bash
pytest tests/unit/
pytest tests/integration/
pytest tests/e2e/
```

---

## 🔧 Environment Variables

Supports `.env` or exported shell variables:

```env
DB_URL=postgresql://username:password@localhost:5432/mydb
S3_BUCKET=my-bucket
S3_KEY=data/users.parquet
S3_ACCESS_KEY=...
S3_SECRET_KEY=...
S3_ENDPOINT=https://s3.amazonaws.com
IAM_ROLE=arn:aws:iam::123456789012:role/MyUnloadRole
```

Generate a template with:

```bash
sqlxport run --generate-env-template
```

---

## 🛠 Roadmap

* ✅ Modular export modes
* ✅ CSV and partitioned output
* ⏳ Add `jsonl`, `xlsx` formats
* ⏳ Plugin system for writers/loaders
* ⏳ SaaS mode / UI platform
* ⏳ Kafka/Kinesis streaming support

---

## 🔐 Security

* Don’t commit `.env` files
* Use credential vaults when possible

---

## 👨‍💼 Author

Vahid Saber  
Built with ❤️ for data engineers and developers.

---

## 📄 License

MIT License
