# sqlxport – Test Plan

This test plan outlines the unit, integration, and E2E coverage targets for sqlxport, aligned with the CLI/API refactor and backend abstraction.

---

## ✅ Unit Tests

| Component             | Coverage Target                       |
|----------------------|----------------------------------------|
| `api/export.py`      | Export modes (query, unload)           |
| `api/ddl.py`         | Schema inference, DDL generation       |
| `api/preview.py`     | File preview via file-query-engine     |
| `query_engines/*`    | DuckDB engine, Trino/Athena stubs      |
| `enums.py`           | ExportMode, FileQueryEngine parsing    |

---

## 🔁 Integration Tests

| Scenario                                | Formats | Engines         | Storage   |
|-----------------------------------------|---------|------------------|-----------|
| Postgres query                          | Parquet, CSV | DuckDB         | S3, MinIO |
| Redshift UNLOAD                         | Parquet     | DuckDB, Athena  | S3        |
| DDL generation for partitioned output   | Parquet     | DuckDB, Athena  | S3        |
| File preview                            | Parquet, CSV | DuckDB         | local     |
| Complex query w/ aliases + joins        | Parquet     | DuckDB, Athena  | S3        |

---

## 🧪 CLI Coverage

| Subcommand       | Flags to Cover                          |
|------------------|------------------------------------------|
| `run`            | `--export-mode`, `--query`, `--output-dir`, `--format` |
| `preview`        | `--file-query-engine`, `--preview-limit` |
| `generate-ddl`   | `--file-query-engine`, `--ddl-output`    |
| `validate-table` | `--file-query-engine`, Glue integration  |
| `env-template`   | No args; check output formatting         |

---

## ✅ Snapshot Testing

- Validate stable output Parquet files using:
  - Row count
  - Schema hash
  - Partition listing
