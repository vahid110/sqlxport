# sqlxport/ddl/utils.py

import pyarrow.parquet as pq

import os

import pyarrow.parquet as pq
import os

def find_first_parquet(path):
    #print(f"[DEBUG] Looking for .parquet files under: {path}")
    if os.path.isfile(path) and path.endswith(".parquet"):
        #print(f"[DEBUG] Found single parquet file: {path}")
        return path
    for root, _, files in os.walk(path):
        for f in files:
            if f.endswith(".parquet"):
                found = os.path.join(root, f)
                #print(f"[DEBUG] Found parquet file: {found}")
                return found
    raise FileNotFoundError(f"No .parquet files found under: {path}")

def generate_athena_ddl(local_parquet_path, s3_prefix, table_name, partition_cols=None):
    real_file = find_first_parquet(local_parquet_path)
    #print(f"[DEBUG] Using Parquet file: {real_file}")

    schema = pq.read_schema(real_file)
    #print(f"[DEBUG] Full schema fields:")
    # for field in schema:
    #     print(f"  - {field.name}: {field.type}")

    ddl = f"CREATE EXTERNAL TABLE IF NOT EXISTS {table_name} (\n"
    for field in schema:
        if partition_cols and field.name in partition_cols:
            #print(f"[DEBUG] Skipping partition column from main schema: {field.name}")
            continue
        athena_type = arrow_to_athena_type(field.type)
        ddl += f"  {field.name} {athena_type},\n"
    ddl = ddl.rstrip(",\n") + "\n)\n"

    if partition_cols:
        ddl += "PARTITIONED BY (\n"
        for col in partition_cols:
            ddl += f"  {col} STRING,\n"
        ddl = ddl.rstrip(",\n") + "\n)\n"

    ddl += "STORED AS PARQUET\n"
    ddl += f"LOCATION '{s3_prefix}';\n"

    #print(f"[DEBUG] Generated DDL:\n" + ddl)
    return ddl




def arrow_to_athena_type(arrow_type):
    import pyarrow as pa

    if pa.types.is_string(arrow_type):
        return "STRING"
    if pa.types.is_boolean(arrow_type):
        return "BOOLEAN"
    if pa.types.is_int8(arrow_type) or pa.types.is_int16(arrow_type):
        return "TINYINT"
    if pa.types.is_int32(arrow_type):
        return "INT"
    if pa.types.is_int64(arrow_type):
        return "BIGINT"
    if pa.types.is_float32(arrow_type):
        return "FLOAT"
    if pa.types.is_float64(arrow_type):
        return "DOUBLE"
    if pa.types.is_decimal(arrow_type):
        return f"DECIMAL({arrow_type.precision}, {arrow_type.scale})"
    if pa.types.is_date(arrow_type):
        return "DATE"
    if pa.types.is_timestamp(arrow_type):
        return "TIMESTAMP"
    if pa.types.is_binary(arrow_type):
        return "VARBINARY"
    if pa.types.is_list(arrow_type):
        return f"ARRAY<{arrow_to_athena_type(arrow_type.value_type)}>"
    if pa.types.is_struct(arrow_type):
        fields = ", ".join(
            f"{f.name}: {arrow_to_athena_type(f.type)}" for f in arrow_type
        )
        return f"STRUCT<{fields}>"
    if pa.types.is_map(arrow_type):
        return f"MAP<STRING, STRING>"  # Simplified, can be improved

    return "STRING"  # Fallback

