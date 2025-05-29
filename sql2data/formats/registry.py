# sql2data/formats/registry.py

from sql2data.formats.parquet_writer import ParquetWriter
from sql2data.formats.base import FormatWriter

# Optional: import CSV writer only if needed
try:
    from sql2data.formats.csv_writer import CsvWriter
except ImportError:
    CsvWriter = None

WRITERS = {
    "parquet": ParquetWriter,
}

if CsvWriter:
    WRITERS["csv"] = CsvWriter


def get_writer(format: str) -> FormatWriter:
    fmt = format.lower()
    if fmt not in WRITERS:
        raise ValueError(f"Unsupported format: {format}. Supported: {', '.join(WRITERS.keys())}")
    return WRITERS[fmt]()
