# Integration Testing: MinIO + Athena

This guide walks you through testing `sqlxport` against a local MinIO instance and AWS Athena using exported data.

---

## 🔧 Part 1: Setup MinIO Locally

### 1. Start MinIO in Docker

```bash
docker run -p 9000:9000 -p 9001:9001 \
  -e "MINIO_ROOT_USER=admin" \
  -e "MINIO_ROOT_PASSWORD=password" \
  quay.io/minio/minio server /data --console-address ":9001"
```

### 2. Create a Bucket (e.g., `data-exports`)

```bash
mc alias set local http://localhost:9000 admin password
mc mb local/data-exports
```

---

## 🧪 Part 2: Export Sample Data

```bash
sqlxport \
  --query "SELECT * FROM logs" \
  --output-dir exports/ \
  --partition-by log_date \
  --upload-output-dir \
  --s3-endpoint http://localhost:9000 \
  --s3-access-key admin \
  --s3-secret-key password \
  --s3-bucket data-exports \
  --s3-key logs
```

---

## 🔍 Part 3: Preview from MinIO

```bash
sqlxport \
  --preview-s3-file \
  --s3-bucket data-exports \
  --s3-key logs/log_date=2024-01-02/part-0000.parquet \
  --s3-endpoint http://localhost:9000 \
  --s3-access-key admin \
  --s3-secret-key password
```

---

## 📜 Part 4: Generate Athena DDL

```bash
Create table:

CREATE TABLE logs (
    id SERIAL PRIMARY KEY,
    message TEXT,
    severity TEXT,
    log_date DATE
);


INSERT INTO logs (message, severity, log_date) VALUES
('Startup complete', 'INFO', '2024-01-01'),
('User login failed', 'WARN', '2024-01-01'),
('Disk space low', 'ERROR', '2024-01-02'),
('Scheduled backup succeeded', 'INFO', '2024-01-02'),
('Unexpected shutdown', 'ERROR', '2024-01-03');

Generate Sample Parquet Files:

sqlxport \
  --query "SELECT * FROM logs" \
  --output-dir exports/ \
  --partition-by log_date

This creates:
exports/
├── log_date=2024-01-01/
│   └── part-0000.parquet
├── log_date=2024-01-02/
│   └── part-0000.parquet
└── log_date=2024-01-03/
    └── part-0000.parquet

Upload to MinIO (S3-Compatible) Test Bucket:
aws --endpoint-url http://localhost:9000 s3 mb s3://test-bucket --profile minio

aws --endpoint-url http://localhost:9000 s3 cp exports/ s3://test-bucket/athena-logs/ --recursive --profile minio

Verify with:

aws --endpoint-url http://localhost:9000 s3 ls s3://test-bucket/athena-logs/ --recursive --profile minio


Generate Athena DDL:

sqlxport \
  --generate-athena-ddl exports/log_date=2024-01-02/part-0000.parquet \
  --athena-s3-prefix s3://data-exports/logs/ \
  --athena-table-name logs \
  --partition-by log_date

This outputs:

CREATE EXTERNAL TABLE logs (
  id INT,
  message STRING,
  severity STRING
)
PARTITIONED BY (
  log_date STRING
)
STORED AS PARQUET
LOCATION 's3://test-bucket/athena-logs/';

```

Paste this DDL in the Athena console and run it.

---

## 🗂 Part 5: MSCK Repair Table

After creating the table in Athena:

```sql
MSCK REPAIR TABLE logs;
```

---

## ✅ Part 6: Run Athena Query

```sql
SELECT * FROM logs WHERE log_date = '2024-01-02';
```

---

Happy querying!