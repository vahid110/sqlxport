# sqlxport/ddl/utils.py

import pyarrow.parquet as pq
import os

def find_first_parquet(path):
    if os.path.isfile(path) and path.endswith(".parquet"):
        return path
    for root, _, files in os.walk(path):
        for f in files:
            if f.endswith(".parquet"):
                return os.path.join(root, f)
    raise FileNotFoundError(f"No .parquet files found under: {path}")

def generate_athena_ddl(local_parquet_path, s3_prefix, table_name, partition_cols=None, schema_df=None):
    import pyarrow.parquet as pq

    if schema_df is None:
        real_file = find_first_parquet(local_parquet_path)
        schema = pq.read_schema(real_file)
        fields = [{"name": f.name, "type": f.type} for f in schema]
        print(f"📦 Inferred schema from {real_file}:")
        for f in fields:
            print(f"  - {f['name']}: {f['type']}")
    else:
        fields = schema_df  # list of dicts: [{name:..., type:...}, ...]

    # Validate partition columns exist in schema
    if partition_cols:
        parquet_field_names = {f["name"] for f in fields}
        missing = set(partition_cols) - parquet_field_names
        if missing:
            raise ValueError(f"❌ Partition columns not found in schema: {missing}")

    # Split fields into partition and non-partition columns
    partition_schema = []
    main_schema = []

    for field in fields:
        name = field["name"]
        typ = field["type"]
        athena_type = arrow_to_athena_type(typ)

        if partition_cols and name in partition_cols:
            partition_schema.append((name, athena_type))
        else:
            main_schema.append((name, athena_type))

    # Start building DDL
    ddl = f"CREATE EXTERNAL TABLE IF NOT EXISTS {table_name} (\n"
    for name, athena_type in main_schema:
        ddl += f"  {name} {athena_type},\n"
    ddl = ddl.rstrip(",\n") + "\n)\n"

    # Add partitioned columns if present
    if partition_schema:
        ddl += "PARTITIONED BY (\n"
        for name, athena_type in partition_schema:
            ddl += f"  {name} {athena_type},\n"
        ddl = ddl.rstrip(",\n") + "\n)\n"

    ddl += "STORED AS PARQUET\n"
    ddl += f"LOCATION '{s3_prefix}';\n"

    print("📄 Generated Athena DDL:\n", ddl)
    return ddl


def arrow_to_athena_type(arrow_type):
    import pyarrow as pa

    if isinstance(arrow_type, str):
        t = arrow_type.lower()
        if "int" in t:
            return "BIGINT"
        if "float" in t:
            return "DOUBLE"
        if "string" in t or "utf8" in t:
            return "STRING"
        if "bool" in t:
            return "BOOLEAN"
        if "timestamp" in t:
            return "TIMESTAMP"
        return "STRING"

    # Original logic for pyarrow schema types
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
        return f"MAP<STRING, STRING>"

    return "STRING"
