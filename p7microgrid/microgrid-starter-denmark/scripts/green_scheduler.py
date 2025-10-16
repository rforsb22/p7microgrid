# scripts/green_scheduler.py
import os, pandas as pd, numpy as np

prices_path = "data/elspot_prices.csv"
co2_path    = "data/co2_intensity.csv"

if not os.path.exists(prices_path):
    raise SystemExit("Run scripts/fetch_energi_prices.py first.")
if not os.path.exists(co2_path):
    raise SystemExit("Run scripts/fetch_co2_intensity.py first.")

P = pd.read_csv(prices_path, parse_dates=["time_dk"]).rename(columns={"time_dk":"time"}).sort_values("time")
if "price_dkk_per_kwh" not in P.columns:
    raise SystemExit("Expected price_dkk_per_kwh in prices CSV.")
C = pd.read_csv(co2_path, parse_dates=["time"]).sort_values("time")

# 15-min merge
df = pd.merge_asof(P[["time","price_dkk_per_kwh"]], C[["time","co2_g_per_kwh"]], on="time",
                   direction="nearest", tolerance=pd.Timedelta("30min"))
df = df.dropna(subset=["price_dkk_per_kwh", "co2_g_per_kwh"]).copy()

# normalize both to 0..1 (lower is better for both)
p = df["price_dkk_per_kwh"]
c = df["co2_g_per_kwh"]
p_norm = (p - p.min()) / (p.max() - p.min() + 1e-9)
c_norm = (c - c.min()) / (c.max() - c.min() + 1e-9)

w_price = float(os.getenv("PRICE_WEIGHT", "0.5"))
w_co2   = float(os.getenv("CO2_WEIGHT", "0.5"))
w_sum   = max(w_price + w_co2, 1e-9)
w_price /= w_sum; w_co2 /= w_sum

df["score"] = w_price * p_norm + w_co2 * c_norm  # lower is better

# select best N slots or by quantile
keep_q = float(os.getenv("GREEN_PERCENTILE", "0.2"))
cut = df["score"].quantile(keep_q)
best = df[df["score"] <= cut].copy()

# save a neat schedule
out = best[["time", "price_dkk_per_kwh", "co2_g_per_kwh", "score"]].sort_values("time")
out_path = "data/green_schedule.csv"
out.to_csv(out_path, index=False)

print(f"Selected {len(out)} slots (<= {int(keep_q*100)}% best score). Weights: price={w_price:.2f}, co2={w_co2:.2f}")
print(out.head(12).to_string(index=False))
print(f"Wrote {out_path}")
