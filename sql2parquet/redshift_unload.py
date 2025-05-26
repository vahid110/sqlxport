#sql2parquet/redshift_unload.py

def run_unload(db_url, query, s3_output_prefix, iam_role):
    import psycopg
    import textwrap

    query_clean = query.strip().rstrip(';').replace('\n', ' ')

    unload_sql = textwrap.dedent(f"""
        UNLOAD ('{query_clean}')
        TO '{s3_output_prefix}'
        IAM_ROLE '{iam_role}'
        FORMAT AS PARQUET;
    """).strip()

    print("📤 Executing UNLOAD:")
    print(unload_sql)

    with psycopg.connect(db_url, client_encoding="UTF8") as conn:
        with conn.cursor() as cur:
            cur.execute(unload_sql)
            print("✅ UNLOAD command executed.")
