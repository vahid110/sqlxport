# sqlxport/core/export_modes.py

def is_redshift_url(db_url: str) -> bool:
    return "redshift.amazonaws.com" in db_url

import click

def validate_export_mode(export_mode: str, db_type: str):
    """Validates that the export mode is compatible with the given DB URL."""
    if not export_mode:
        raise click.UsageError("Missing --export-mode")

    export_mode = export_mode.lower()
    db_type = db_type.lower() if db_type else ""

    supported_modes = {
        "redshift-unload": ["redshift"],
        "postgres-query": ["postgresql", "postgres"],
        "mysql-query": ["mysql"],
        "sqlite-query": ["sqlite"],
    }

    if export_mode not in supported_modes:
        raise click.UsageError(f"Unsupported export mode '{export_mode}'")

    valid_prefixes = supported_modes[export_mode]

    if export_mode == "redshift-unload":
        if is_redshift_url(db_type):
            return
        raise click.UsageError(f"Export mode '{export_mode}' is not compatible with DB URL: '{db_type}'")

    if not any(db_type.startswith(p) for p in valid_prefixes):
        raise click.UsageError(f"Export mode '{export_mode}' is not compatible with DB URL: '{db_type}'")

    return True

