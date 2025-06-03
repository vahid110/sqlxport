# Export PostgreSQL to Parquet (Manual vs sqlxport)

This example demonstrates exporting a PostgreSQL query result to a Parquet file.

## Prerequisites

- PostgreSQL running locally
- A table named `users`
- Python + `pandas`, `pyarrow` (for manual)
- `sqlxport` installed (for CLI example)

## Sample Table

```sql
CREATE TABLE users (
  id SERIAL PRIMARY KEY,
  name TEXT,
  email TEXT,
  age INT
);

INSERT INTO users (name, email, age) VALUES
  ('Alice', 'alice@example.com', 30),
  ('Bob', 'bob@example.com', 25),
  ('Charlie', 'charlie@example.com', 40);
```

## Option 1: Manual Python Code

```bash
python run_manual.py
```
## Option 2: sqlxport CLI

```bash
bash run_sqlxport.sh
```

## Output
```
users.parquet
```