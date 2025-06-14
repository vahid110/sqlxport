# sqlxport/cli/main.py
import click
from importlib.metadata import version, PackageNotFoundError

from sqlxport.cli.cmd_export import export
from sqlxport.cli.cmd_preview import preview
from sqlxport.cli.cmd_postprocess import postprocess
from sqlxport.cli.cmd_generate_env import generate_env
from sqlxport.cli.cmd_generate_ddl import generate_ddl
from sqlxport.cli.cmd_validate_table import validate_table
from sqlxport.cli.cmd_generate_ddl import generate_ddl

try:
    __version__ = version("sqlxport")
except PackageNotFoundError:
    __version__ = "unknown"

@click.group()
@click.version_option(__version__, "--version", "-v", message="%(version)s")
def cli():
    """sqlxport: Export SQL query results to Parquet or CSV with optional S3 + Glue/Athena integration."""
    pass

cli.add_command(export)
cli.add_command(preview)
cli.add_command(postprocess)
cli.add_command(generate_env)
cli.add_command(generate_ddl)
cli.add_command(validate_table)
cli.add_command(generate_ddl)

if __name__ == "__main__":
    cli()
