import os
import math
from pathlib import Path
import pandas as pd
import numpy as np
from dotenv import load_dotenv

DATA = Path("data")

def read_series(path: Path, time_col="time", value_col=None):
    if not path.exists():
        return None
    df = pd.read_csv(path, parse_dates=[time_col]).sort_values(time_col)
    if value_col and value_col not in df.columns:
        raise SystemExit(f"{path} missing '{value_col}' column. Columns: {list(df.columns)}")
    return df

def merge_power_sources():
    wind = read_series(DATA/"weather_wind.csv", time_col="time", value_col=None)
    solar = read_series(DATA/"solar_pv.csv", time_col="time", value_col=None)

    if wind is None and solar is None:
        raise SystemExit("No renewables found. Run scripts/fetch_weather.py and scripts/fetch_sun.py first.")

    frames = []
    if wind is not None:
        # expect wind_kw
        if "wind_kw" not in wind.columns:
            raise SystemExit("weather_wind.csv must have 'wind_kw'")
        frames.append(wind[["time","wind_kw"]])
    if solar is not None:
        if "pv_kw" not in solar.columns:
            raise SystemExit("solar_pv.csv must have 'pv_kw'")
        frames.append(solar[["time","pv_kw"]])

    # Outer join then fill missing with 0
    df = None
    for f in frames:
        df = f if df is None else pd.merge_asof(
            df.sort_values("time"),
            f.sort_values("time"),
            on="time", direction="nearest", tolerance=pd.Timedelta("30min")
        )

    df = df.fillna(0.0).sort_values("time").reset_index(drop=True)
    if "wind_kw" not in df.columns: df["wind_kw"] = 0.0
    if "pv_kw" not in df.columns: df["pv_kw"] = 0.0
    df["gen_kw"] = df["wind_kw"].astype(float) + df["pv_kw"].astype(float)

    # infer step in hours (default 0.25 if irregular)
    if len(df) >= 2:
        dt = (df["time"].iloc[1] - df["time"].iloc[0]).total_seconds() / 3600.0
        if dt <= 0 or dt > 2.0:
            dt = 0.25
    else:
        dt = 0.25
    df["dt_h"] = dt

    return df

def simulate_battery(df, cap_kwh, max_c_kw, max_d_kw, eta_roundtrip, print_timeline=False,
                     toaster_kw=1.2, cycle_min=2):
    # Split round-trip into symmetric charge/discharge efficiencies
    eta_c = eta_d = math.sqrt(max(min(eta_roundtrip, 0.9999), 0.01))

    soc = 0.0  # kWh
    soc_series = []
    toaster_events = []  # (time, 'cycle')
    dt_h = float(df["dt_h"].iloc[0])

    # First pass: charge from renewables only, respecting limits
    for i, row in df.iterrows():
        gen_kw = float(row["gen_kw"])
        # charge power capped by max and what fits in the battery over this step
        room_kwh = cap_kwh - soc
        max_store_kwh = max(room_kwh, 0.0)
        cap_from_power_kwh = max_c_kw * dt_h * eta_c
        inflow_kwh = min(gen_kw * dt_h * eta_c, cap_from_power_kwh, max_store_kwh)
        soc += inflow_kwh
        soc_series.append(soc)

    # Available energy for toaster after charging
    available_kwh = soc

    # Compute how many cycles you can run *right now* purely from battery
    cycle_h = cycle_min / 60.0
    energy_per_cycle = toaster_kw * cycle_h  # kWh
    max_cycles = int(available_kwh // energy_per_cycle) if energy_per_cycle > 0 else 0
    total_minutes = max_cycles * cycle_min

    # Optional: simulate actually running the toaster greedily after the last time point
    # (uses only battery; no more generation)
    if print_timeline and max_cycles > 0:
        # we assume we start using right after the last timestamp
        t0 = df["time"].iloc[-1]
        soc2 = available_kwh
        for k in range(max_cycles):
            # discharge limit per step: we simulate exact cycle length, not the df step
            # convert discharge cap to kWh over the cycle duration
            allowed_kwh = max_d_kw * cycle_h / eta_d
            use_kwh = min(energy_per_cycle / eta_d, soc2, allowed_kwh)
            if use_kwh + 1e-9 < energy_per_cycle / eta_d:
                # cannot supply full cycle within limits -> stop
                break
            soc2 -= energy_per_cycle / eta_d
            toaster_events.append((t0 + pd.Timedelta(minutes=(k+1)*cycle_min), "toast"))

    summary = {
        "battery_capacity_kwh": cap_kwh,
        "battery_soc_kwh": available_kwh,
        "toaster_kw": toaster_kw,
        "toaster_cycle_min": cycle_min,
        "energy_per_cycle_kwh": energy_per_cycle,
        "max_cycles_from_soc": max_cycles,
        "max_minutes_from_soc": total_minutes,
        "time_step_minutes": int(df["dt_h"].iloc[0] * 60),
    }

    return pd.Series(soc_series, index=df["time"], name="soc_kwh"), summary, toaster_events

if __name__ == "__main__":
    load_dotenv()

    # Read renewables
    df = merge_power_sources()

    # Config
    cap_kwh = float(os.getenv("BATTERY_MAX_KWH", "10"))
    max_c_kw = float(os.getenv("BATTERY_MAX_CHARGE_KW", "5"))
    max_d_kw = float(os.getenv("BATTERY_MAX_DISCHARGE_KW", "5"))
    eta = float(os.getenv("BATTERY_EFFICIENCY", "0.92"))
    toaster_kw = float(os.getenv("TOASTER_KW", "1.2"))
    cycle_min = float(os.getenv("TOASTER_CYCLE_MIN", "2"))

    soc_series, summary, events = simulate_battery(
        df, cap_kwh, max_c_kw, max_d_kw, eta, print_timeline=True,
        toaster_kw=toaster_kw, cycle_min=cycle_min
    )

    # Save a quick CSV with SOC
    out_soc = DATA/"battery_soc_from_renewables.csv"
    soc_series.reset_index().to_csv(out_soc, index=False)

    print("\n=== Battery from Renewables (no other loads) ===")
    print(f"Time step: {summary['time_step_minutes']} min")
    print(f"Capacity:  {summary['battery_capacity_kwh']:.2f} kWh")
    print(f"SOC now:   {summary['battery_soc_kwh']:.2f} kWh")
    print(f"Toaster:   {summary['toaster_kw']} kW, {summary['toaster_cycle_min']} min/cycle "
          f"({summary['energy_per_cycle_kwh']:.3f} kWh/cycle)")

    print("\n>>> AVAILABLE TOASTER USAGE FROM STORED ENERGY ONLY:")
    print(f"Max cycles:  {summary['max_cycles_from_soc']} cycles")
    print(f"Total time:  {summary['max_minutes_from_soc']} minutes")

    if events:
        print("\nFirst few toaster completions (simulated):")
        for t, tag in events[:5]:
            print(f"  {t} -> {tag}")

    print(f"\nWrote SOC timeline: {out_soc}")
