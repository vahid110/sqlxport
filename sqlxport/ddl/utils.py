# sqlxport/ddl/utils.py

import pyarrow.parquet as pq
import os

def generate_athena_ddl(local_parquet_path, s3_prefix, table_name="my_table", partition_cols=None):
    table = pq.ParquetFile(local_parquet_path).read()
    schema = table.schema

    ddl = f"CREATE EXTERNAL TABLE {table_name} (\n"
    for field in schema:
        if partition_cols and field.name in partition_cols:
            continue  # skip partition columns from main schema
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

