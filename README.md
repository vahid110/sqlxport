[![codecov](https://codecov.io/gh/vahid110/sqlxport/graph/badge.svg?token=LUDLBXTE9S)](https://codecov.io/gh/vahid110/sqlxport)
![CI](https://github.com/vahid110/sqlxport/actions/workflows/ci.yml/badge.svg)

# sqlxport

**Modular CLI tool to extract data from PostgreSQL/Redshift and export to various formats (e.g. Parquet, CSV), with optional S3 upload and Athena integration.**

---

## ✅ Features

- 🔄 Run custom SQL queries against PostgreSQL or Redshift
- 📦 Export to Parquet or CSV (`--format`)
- 🪣 Upload results to S3 or MinIO
- 🔄 Redshift `UNLOAD` support
- 🧩 Partition output by column
- 📜 Generate Athena `CREATE TABLE` DDL
- 🔍 Preview local or remote Parquet/CSV files
- ⚙️ `.env` support for convenient config

---

## 📦 Installation

```bash
pip install .
# or editable install
pip install -e .
```

---

## 🚀 Usage

### Basic

```bash
sqlxport run \
  --db-url postgresql://user:pass@localhost:5432/mydb \
  --query "SELECT * FROM users" \
  --output-file users.parquet \
  --format parquet
```

### With S3 Upload

```bash
sqlxport run \
  --db-url postgresql://... \
  --query "..." \
  --output-file users.parquet \
  --s3-bucket my-bucket \
  --s3-key users.parquet \
  --s3-access-key AKIA... \
  --s3-secret-key ... \
  --s3-endpoint https://s3.amazonaws.com
```

### Partitioned Export

```bash
sqlxport run \
  --db-url postgresql://... \
  --query "..." \
  --output-dir output/ \
  --partition-by group_column
```

### Redshift UNLOAD Mode

```bash
sqlxport run \
  --use-redshift-unload \
  --db-url redshift+psycopg2://... \
  --query "SELECT * FROM large_table" \
  --s3-output-prefix s3://bucket/unload/ \
  --iam-role arn:aws:iam::123456789012:role/MyUnloadRole
```

---

## 🧪 Running Tests

```bash
pytest -v
```

---

## 🧬 Environment Variables

You can set options via `.env` or environment:

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

- ✅ Modular format support
- ✅ CSV support
- ⏳ Add `jsonl`, `xlsx` formats
- ⏳ Plugin system for custom writers/loaders
- ⏳ SaaS mode or server-side export platform
- ⏳ Stream output to Kafka/Kinesis

---

## 🔐 Security

* Don't commit `.env` files
* Store credentials securely (e.g. `.aws/credentials`, vaults)

---

## 👨‍💻 Author

Vahid Saber  
Built with ❤️ for data engineers and developers.

---

## 📄 License

MIT License
