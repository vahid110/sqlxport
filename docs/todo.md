# sqlxport – June 2025 To-Do

This document tracks the weekly milestones and tasks for refactoring, testing, and feature enhancements in sqlxport.

---

## ✅ Week 4 (June 3–9): CLI Redesign + Abstraction Foundations

- [x] Add CLI subcommands:
  - [x] `run`
  - [x] `preview`
  - [x] `generate-ddl`
  - [x] `env-template`
  - [x] `validate-table`
- [x] Introduce `--export-mode` (e.g., `query`, `unload`)
- [x] Introduce `--file-query-engine` (default = `duckdb`)
- [x] Update CLI help and usage examples

---

## 🔧 Week 5 (June 10–16): API Refactor & Engine Abstraction

- [ ] Move core logic to `sqlxport/api/`:
  - [ ] `export.py`
  - [ ] `ddl.py`
  - [ ] `preview.py`
- [ ] Define enums:
  - [ ] `ExportMode`
  - [ ] `FileQueryEngine`
- [ ] Implement `QueryEngine` interface
  - [ ] Add `DuckDBEngine` implementation
  - [ ] Wire `--file-query-engine` to engine router
- [ ] Update CLI to call new API methods
- [ ] Add unit tests for new API paths

---

## 🔁 Week 6 (June 17–23): Refactor Tests and Demos

- [ ] Refactor all unit tests to use new API
- [ ] Refactor integration tests to use new CLI (`run`, `preview`, etc.)
- [ ] Update all demo scripts:
  - [ ] `examples/`
  - [ ] `demo/`
- [ ] Ensure test coverage for:
  - [ ] Redshift unload
  - [ ] Postgres query
  - [ ] Parquet and CSV formats
  - [ ] S3 and MinIO destinations
  - [ ] Preview and DDL generation paths
- [ ] Create/update `test_plan.md`

---

## 🧠 Week 7 (June 24–30): Complex Query Testing + Multi-Engine Validation

- [ ] Add complex query test suite:
  - [ ] WITH clause
  - [ ] Joins
  - [ ] Subqueries and aliases
- [ ] Update schema inference to handle aliases/nested columns
- [ ] Add Athena-based validation:
  - [ ] Glue DDL for complex schema
  - [ ] MSCK REPAIR TABLE
  - [ ] SELECT partition/count test
- [ ] Add `examples/complex_postgres_query/` demo
- [ ] Optional: Add `TrinoEngine` stub for `--file-query-engine`

---

## Stretch Goals (Post-June)

- [ ] Plugin registry for custom `file-query-engine`s
- [ ] Pro Edition planning (multi-tenant, UI, auth, etc.)
- [ ] SDK generation (Java/Scala clients via API)
