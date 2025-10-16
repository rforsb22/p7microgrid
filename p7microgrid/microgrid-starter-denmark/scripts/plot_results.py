import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path

prices_path = Path("data/elspot_prices.csv")
co2_path    = Path("data/co2_intensity.csv")
wind_path   = Path("data/weather_wind.csv")
sim_path    = Path("data/simulation_log.csv")
green_path  = Path("data/green_schedule.csv")

for p in (prices_path, sim_path):
    if not p.exists():
        raise SystemExit("Missing CSVs. Run fetch_energi_prices.py and run_sim.py first.")

# prices (DKK/kWh)
prices = pd.read_csv(prices_path, parse_dates=["time_dk"]).rename(columns={"time_dk":"time"})[["time","price_dkk_per_kwh"]].sort_values("time")

# co2 (optional but recommended)
co2 = pd.read_csv(co2_path, parse_dates=["time"]).sort_values("time") if co2_path.exists() else None

# sim
sim = pd.read_csv(sim_path, parse_dates=["time"]).sort_values("time")

# merge price + co2
df = prices.copy()
if co2 is not None and not co2.empty:
    df = pd.merge_asof(df, co2, on="time", direction="nearest", tolerance=pd.Timedelta("30min"))

# main figure: price + CO2
fig, ax1 = plt.subplots()
ax1.set_title("DK Price & CO₂")
ax1.set_xlabel("Time")
ax1.set_ylabel("DKK/kWh")
ax1.plot(df["time"], df["price_dkk_per_kwh"], label="Price (DKK/kWh)")

# thresholds for price shading
cheap_q = df["price_dkk_per_kwh"].quantile(0.25)
exp_q   = df["price_dkk_per_kwh"].quantile(0.75)
ymin, ymax = df["price_dkk_per_kwh"].min(), df["price_dkk_per_kwh"].max()
ax1.axhspan(ymin, cheap_q, alpha=0.15)
ax1.axhspan(exp_q, ymax,  alpha=0.10)
ax1.axhline(cheap_q, linestyle="--")
ax1.axhline(exp_q,   linestyle="--")

# CO2 on right axis
if "co2_g_per_kwh" in df.columns and df["co2_g_per_kwh"].notna().any():
    ax2 = ax1.twinx()
    ax2.set_ylabel("CO₂ (g/kWh)")
    ax2.plot(df["time"], df["co2_g_per_kwh"], label="CO₂ (g/kWh)")
    ax2.grid(False)

# green markers (robust to _x/_y suffixes)
if green_path.exists():
    g = pd.read_csv(green_path, parse_dates=["time"]).sort_values("time")
    g = g.merge(df[["time", "price_dkk_per_kwh"]], on="time", how="inner")

    # pick whichever price column exists after merge
    gpcol = None
    for cand in ["price_dkk_per_kwh", "price_dkk_per_kwh_x", "price_dkk_per_kwh_y"]:
        if cand in g.columns:
            gpcol = cand
            break
    if gpcol is None:
        # last resort: any column starting with the base name
        cands = [c for c in g.columns if c.startswith("price_dkk_per_kwh")]
        if cands:
            gpcol = cands[0]

    if gpcol is not None and not g.empty:
        ax1.scatter(g["time"], g[gpcol], s=25, label="Best (cheap+clean)")


# legends
lines, labels = ax1.get_legend_handles_labels()
if "co2_g_per_kwh" in df.columns:
    l2, lb2 = ax2.get_legend_handles_labels()
    lines += l2; labels += lb2
ax1.legend(lines, labels, loc="upper left")

fig.autofmt_xdate(); fig.tight_layout(); plt.show()
