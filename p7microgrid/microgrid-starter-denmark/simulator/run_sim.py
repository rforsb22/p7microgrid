# --- top of file (unchanged imports) ---
import os, yaml, pandas as pd, numpy as np
from dotenv import load_dotenv
from pymgrid import Microgrid
from pymgrid.modules import BatteryModule, LoadModule, RenewableModule

ROOT = os.path.dirname(os.path.dirname(__file__))

def load_config():
    with open(os.path.join(ROOT,"configs","microgrid.yaml")) as f:
        return yaml.safe_load(os.path.expandvars(f.read()))

def read_csv(path, **kw):
    return pd.read_csv(path, **kw) if os.path.exists(path) else None

def to_naive_local(s):
    s = pd.to_datetime(s)
    # if tz-aware, drop tz; if tz-naive, this is a no-op
    return s.dt.tz_convert(None) if getattr(s.dt, "tz", None) is not None else s

def synthesize_load(n, base_kw=8.0):
    t = np.arange(n); day_frac = (t % 24) / 24.0
    load = base_kw * (0.6 + 0.4 * (np.sin(2*np.pi*(day_frac-0.2))**2))
    load += np.random.normal(0, 0.4, size=n)
    return np.maximum(load, 0.5)

def main():
    load_dotenv(); cfg = load_config()


    # --- DKK prices at 15-min or hourly ---
    prices = read_csv(os.path.join(ROOT, "data", "elspot_prices.csv"), parse_dates=["time_dk"])
    if prices is None or prices.empty:
        raise SystemExit("Run scripts/fetch_energi_prices.py first.")
    prices = prices.rename(columns={"time_dk": "time"})[["time", "price_dkk_per_kwh"]].sort_values("time")
    df = prices.copy()

    # after df is created from prices ...
    solar = read_csv(os.path.join(ROOT, "data", "solar_pv.csv"), parse_dates=["time"])
    if solar is not None and not solar.empty:
        df = pd.merge_asof(
            df.sort_values("time"),
            solar[["time", "pv_kw"]].sort_values("time"),
            on="time",
            direction="nearest",
            tolerance=pd.Timedelta("30min"),
        )
        df["pv_kw"] = df["pv_kw"].fillna(0.0)
    else:
        df["pv_kw"] = 0.0

    wind = read_csv(os.path.join(ROOT, "data", "weather_wind.csv"), parse_dates=["time"])
    if wind is not None and not wind.empty:
        df = pd.merge_asof(
            df.sort_values("time"),
            wind[["time", "wind_kw"]].sort_values("time"),
            on="time",
            direction="nearest",
            tolerance=pd.Timedelta("30min"),
        )
        df["wind_kw"] = df["wind_kw"].fillna(0.0)
    else:
        df["wind_kw"] = 0.0



    n = len(df)

    # Battery
    battery = BatteryModule(
        min_capacity=0.0,
        max_capacity=float(cfg["battery"]["max_capacity_kwh"]),
        max_charge=float(cfg["battery"]["max_charge_kw"]),
        max_discharge=float(cfg["battery"]["max_discharge_kw"]),
        efficiency=float(cfg["battery"]["efficiency"]),
        init_soc=float(cfg["battery"]["init_soc"]),  # <-- required: pick one of init_soc or init_charge
    )



    # Renewables
    renewable = RenewableModule(
        time_series=(df["wind_kw"].fillna(0.0) + df["pv_kw"].fillna(0.0)).astype(float).values
    )

    # Load
    load = LoadModule(
        time_series=synthesize_load(n).astype(float)
    )

    mg = Microgrid([
        ("battery", battery),
        ("renewable", renewable),
        ("load", load),
    ])



    mg.reset()

    # Price-aware policy using DKK
    cheap_q = df["price_dkk_per_kwh"].quantile(0.25)
    exp_q = df["price_dkk_per_kwh"].quantile(0.75)

    rows = []
    for i in range(n):
        p = float(df.iloc[i]["price_dkk_per_kwh"])
        if p <= cheap_q:
            batt_cmd = 1.0
        elif p >= exp_q:
            batt_cmd = 0.0
        else:
            batt_cmd = 0.5

        action = {"battery": [batt_cmd]}
        obs, reward, done, info = mg.step(action)

        log = mg.get_log(drop_singleton_key=True).iloc[-1].to_dict()
        log["time"] = df.iloc[i]["time"]
        log["price_dkk_per_kwh"] = p
        log["wind_kw"] = float(df.iloc[i].get("wind_kw", 0.0))
        log["batt_cmd"] = batt_cmd
        rows.append(log)
        if done: break

    out = pd.DataFrame(rows)
    out_path = os.path.join(ROOT, "data", "simulation_log.csv")
    out.to_csv(out_path, index=False)
    print(f"Simulation complete. Log: {out_path}")

    out = pd.DataFrame(rows); out_path=os.path.join(ROOT,"data","simulation_log.csv"); out.to_csv(out_path, index=False)
    print(f"Simulation complete. Log: {out_path}")

if __name__ == "__main__":
    main()
