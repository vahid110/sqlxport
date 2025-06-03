# sqlxport/formats/registry.py

from sqlxport.formats.parquet_writer import ParquetWriter
from sqlxport.formats.base import FormatWriter

# Optional: import CSV writer only if needed
try:
    from sqlxport.formats.csv_writer import CsvWriter
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
