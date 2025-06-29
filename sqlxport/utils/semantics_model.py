import pandas as pd
from joblib import load
from sqlxport.utils.semantics_features import extract_features

_model = None

def load_model():
    global _model
    if _model is None:
        import os
        from pathlib import Path

        def get_model_path():
            return Path(__file__).resolve().parent.parent / "assets" / "semantics_model.pkl"

        _model = load(get_model_path())

    return _model

def classify_column_semantic_label(series: pd.Series) -> str:
    model = load_model()
    features = extract_features(series)
    return model.predict(features)[0]
