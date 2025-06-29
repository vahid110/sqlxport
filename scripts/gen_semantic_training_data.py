import pandas as pd
import numpy as np

def generate_samples(label, generator_fn, n=100):
    return pd.DataFrame({
        "value": [generator_fn() for _ in range(n)],
        "label": label
    })

data = pd.concat([
    generate_samples("email", lambda: f"user{np.random.randint(1,1000)}@example.com"),
    generate_samples("uuid", lambda: str(pd.util.testing.rands(36))),
    generate_samples("zip_code", lambda: f"{np.random.randint(10000, 99999)}"),
    generate_samples("currency", lambda: f"{np.random.randint(1, 5000)}.{np.random.randint(0, 99):02}"),
    generate_samples("hash", lambda: "".join(np.random.choice(list("abcdef0123456789"), 64))),
    generate_samples("date_string", lambda: f"202{np.random.randint(0, 5)}-{np.random.randint(1, 13):02}-{np.random.randint(1, 28):02}"),
    generate_samples("free_text", lambda: np.random.choice(["hello world", "sample text", "note content"])),
    generate_samples("numeric_id", lambda: str(np.random.randint(100000, 999999))),
    generate_samples("unknown", lambda: "")  # intentionally ambiguous
])

data.to_csv("assets/semantic_training.csv", index=False)
print("✅ Saved: assets/semantic_training.csv")
