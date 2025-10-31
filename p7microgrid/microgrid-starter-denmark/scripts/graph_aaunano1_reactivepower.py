# python
from pathlib import Path
import importlib.util
import json
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

# safely choose a style: prefer seaborn if available, otherwise fallback
try:
    plt.style.use("seaborn-whitegrid")
except Exception:
    if importlib.util.find_spec("seaborn") is not None:
        import seaborn as sns
        plt.style.use("seaborn")
    else:
        plt.style.use("default")

# compute path relative to this script
data_file = Path(__file__).resolve().parent.parent / "data" / "etsmartplug_aaunano1_very_heavy_load_2k_tokens.json"
if not data_file.exists():
    raise FileNotFoundError(f"Data file not found: {data_file}")

with data_file.open("r", encoding="utf-8") as f:
    data = json.load(f)

# build DataFrame and keep original unix timestamps (no conversion)
cols = data["cols"]
df = pd.DataFrame(data["rows"], columns=cols)

# ensure numeric types
df["reactivepower"] = pd.to_numeric(df["reactivepower"], errors="coerce")
df["timeinstant"] = pd.to_numeric(df["timeinstant"], errors="coerce")

# drop bad rows and sort by the unix timestamp (kept as-is)
df = df.dropna(subset=["reactivepower", "timeinstant"]).sort_values("timeinstant")

# aggregate identical timestamps (many identical ms entries)
df_ts = df.groupby("timeinstant")["reactivepower"].mean().sort_index()

# detect spikes using z-score
mean = df_ts.mean()
std = df_ts.std()
z = (df_ts - mean) / std
threshold = 2.0
spikes = df_ts[z.abs() > threshold]

# plot keeping original unix timestamps on x-axis
plt.figure(figsize=(12, 5))
plt.plot(df_ts.index, df_ts.values, label="reactivepower (aggregated by timeinstant)", lw=1)
plt.scatter(spikes.index, spikes.values, color="red", s=30, label=f"spikes (|z|>{threshold})")
plt.xlabel("unix timestamp (ms) — kept original")
plt.ylabel("reactive power")
plt.title("Reactive power time series (original unix timestamps)")
plt.legend()
plt.tight_layout()
plt.savefig("reactivepower_spikes_unix_ts.png", dpi=150)
plt.show()
