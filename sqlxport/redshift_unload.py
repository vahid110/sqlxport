def run_unload(db_url, query, s3_output_prefix, iam_role):
    import psycopg

    from sqlxport.redshift_unload import generate_unload_statement

    # Clean query and generate UNLOAD SQL
    query_clean = query.strip().rstrip(';').replace('\n', ' ')
    unload_sql = generate_unload_statement(query_clean, s3_output_prefix, iam_role, format="parquet")

    print("📤 Executing UNLOAD:")
    print(unload_sql)

    with psycopg.connect(db_url, client_encoding="UTF8") as conn:
        with conn.cursor() as cur:
            cur.execute(unload_sql)
            print("✅ UNLOAD command executed.")

def generate_unload_statement(query: str, s3_path: str, iam_role: str, format: str = "parquet", options: list[str] = None) -> str:
    format_clause = {
        "parquet": "FORMAT AS PARQUET",
        "csv": "FORMAT AS CSV",
    }.get(format.lower(), None)

    if not format_clause:
        raise ValueError(f"Unsupported format: {format}")

    opts = " ".join(options or [])
    return f"""UNLOAD ('{query}') TO '{s3_path}' IAM_ROLE '{iam_role}' {format_clause} {opts};"""
