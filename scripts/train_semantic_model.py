import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestClassifier
import os
from joblib import dump
from sqlxport.utils.semantics_features import extract_features

def gen(label, fn, n=100):
    return pd.DataFrame({"value": [fn() for _ in range(n)], "label": label})

data = pd.concat([
    gen("email", lambda: f"user{np.random.randint(1,9999)}@example.com"),
    gen("uuid", lambda: f"{np.random.bytes(16).hex()}"),
    gen("zip_code", lambda: f"{np.random.randint(10000,99999)}"),
    gen("currency", lambda: f"{np.random.randint(1,9999)}.{np.random.randint(0,99):02}"),
    gen("hash", lambda: "".join(np.random.choice(list("abcdef0123456789"), 64))),
    gen("date_string", lambda: f"202{np.random.randint(0,4)}-{np.random.randint(1,13):02}-{np.random.randint(1,29):02}"),
    gen("free_text", lambda: np.random.choice(["note text", "hello world", "sample entry"])),
    gen("numeric_id", lambda: str(np.random.randint(100000, 999999))),
    gen("unknown", lambda: "???")
])

X = []
y = []

for label, group in data.groupby("label"):
    series = group["value"]
    features = extract_features(series)
    X.append(features)
    y.append(label)

X_all = pd.concat(X, ignore_index=True)
y_all = y

clf = RandomForestClassifier(n_estimators=100, random_state=42)
clf.fit(X_all, y_all)

os.makedirs("sqlxport/assets", exist_ok=True)
dump(clf, "sqlxport/assets/semantics_model.pkl")
print("✅ Model saved to sqlxport/assets/semantics_model.pkl")
