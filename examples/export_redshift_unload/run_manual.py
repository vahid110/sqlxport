import psycopg
import textwrap

db_url = "redshift+psycopg2://awsuser:pass@endpoint:5439/dev"
query = "SELECT * FROM events"
s3_path = "s3://my-bucket/unload/events_"
iam_role = "arn:aws:iam::123456789012:role/MyUnloadRole"

unload_sql = textwrap.dedent(f"""
    UNLOAD ('{query}')
    TO '{s3_path}'
    IAM_ROLE '{iam_role}'
    FORMAT AS PARQUET;
""")

print("📤 Executing UNLOAD:")
print(unload_sql)

with psycopg.connect(db_url) as conn:
    with conn.cursor() as cur:
        cur.execute(unload_sql)
        print("✅ UNLOAD complete.")