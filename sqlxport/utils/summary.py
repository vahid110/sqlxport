import pandas as pd
import pyarrow.parquet as pq
import pyarrow.csv as pacsv
import os

from sqlxport.utils.semantics_model import classify_column_semantic_label

def summarize_file(file_path: str, max_rows: int = 1000) -> str:
    # Step 1: Load file
    if file_path.endswith(".parquet"):
        table = pq.read_table(file_path)
    elif file_path.endswith(".csv"):
        table = pacsv.read_csv(file_path)
    else:
        return f"❌ Unsupported file type: {file_path}"

    df = table.to_pandas().head(max_rows)
    num_rows, num_cols = df.shape

    lines = [f"📊 The dataset has ~{len(df):,} rows and {num_cols} columns."]

    for col in df.columns:
        series = df[col]
        n_nulls = series.isnull().sum()
        null_pct = (n_nulls / len(df)) * 100
        dtype = series.dtype

        line = f"- `{col}`: "

        if pd.api.types.is_numeric_dtype(dtype):
            line += f"numeric range {series.min()}–{series.max()}"
        elif pd.api.types.is_datetime64_any_dtype(dtype):
            line += f"timestamps from {series.min().date()} to {series.max().date()}"
        elif pd.api.types.is_bool_dtype(dtype):
            true_count = (series == True).sum()
            false_count = (series == False).sum()
            line += f"boolean (✓: {true_count}, ✗: {false_count})"
        else:
            n_unique = series.nunique(dropna=True)
            sample_values = series.dropna().astype(str).unique()[:3]
            line += f"{n_unique} unique values. Examples: {', '.join(sample_values)}"

        # 🧠 ML-based semantic classification
        sem_label = classify_column_semantic_label(series)
        if sem_label and sem_label != "unknown":
            line += f" (semantic: {sem_label})"

        if null_pct > 0:
            line += f" ({null_pct:.1f}% nulls)"

        lines.append(line)

    return "\n".join(lines)
