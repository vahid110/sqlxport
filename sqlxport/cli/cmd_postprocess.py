# sqlxport/cli/cmd_postprocess.py
import click
from sqlxport.cli.glue_ops import (
    register_table_in_glue,
    repair_partitions_in_glue,
    validate_glue_table,
)

@click.command("postprocess")
@click.option('--glue-register', is_flag=True, help='Register table in Glue via Athena')
@click.option('--repair-partitions', is_flag=True, help='Run MSCK REPAIR TABLE after registration')
@click.option('--validate-table', is_flag=True, help='Run validation query after repair')
@click.option('--athena-database', required=True, help='Glue/Athena database name')
@click.option('--athena-table-name', default="my_table", help='Table name to register/repair/validate')
@click.option('--athena-output', required=True, help='S3 location for Athena query results')
@click.option('--region', default="us-east-1", help='AWS region')
def postprocess(glue_register, repair_partitions, validate_table,
                athena_database, athena_table_name, athena_output, region):
    """Run Glue + Athena registration, repair, or validation."""

    if glue_register:
        register_table_in_glue(
            region=region,
            ddl_path="glue_table.sql",
            database=athena_database,
            output=athena_output
        )

    if repair_partitions:
        repair_partitions_in_glue(
            region=region,
            table_name=athena_table_name,
            database=athena_database,
            output=athena_output
        )


    if validate_table:
        validate_glue_table(
            region=region,
            table_name=athena_table_name,
            database=athena_database,
            output=athena_output
        )
