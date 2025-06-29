import pandas as pd

def extract_features(series: pd.Series) -> pd.DataFrame:
    series = series.dropna().astype(str).head(20)

    total_chars = series.str.len().sum() or 1

    features = {
        "avg_len": series.str.len().mean(),
        "pct_digits": series.str.count(r"\d").sum() / total_chars,
        "pct_alpha": series.str.count(r"[A-Za-z]").sum() / total_chars,
        "pct_special": series.str.count(r"[^\w]").sum() / total_chars,
        "avg_entropy": series.map(lambda x: len(set(x)) / max(len(x), 1)).mean(),
        "pct_email": series.str.contains(r"@").mean(),
        "pct_uuid": series.str.contains(r"[a-f0-9\-]{36}").mean(),
        "pct_currency": series.str.contains(r"^\d+\.\d{2}$").mean(),
        "pct_hash": series.str.contains(r"^[a-f0-9]{64}$").mean(),
        "pct_zip": series.str.contains(r"^\d{5}$").mean(),
        "uniqueness": series.nunique() / len(series)
    }

    return pd.DataFrame([features])
