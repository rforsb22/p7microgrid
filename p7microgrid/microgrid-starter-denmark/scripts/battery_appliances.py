import os, math
from pathlib import Path
import pandas as pd
from dotenv import load_dotenv

DATA = Path("data")

def read_series(path: Path, time_col="time"):
    if not path.exists():
        return None
    df = pd.read_csv(path, parse_dates=[time_col]).sort_values(time_col)
    return df

def infer_dt_hours(times: pd.Series, default=0.25):
    if len(times) >= 2:
        dt = (times.iloc[1] - times.iloc[0]).total_seconds()/3600.0
        if 0 < dt <= 2.0:
            return dt
    return default

def watts_from_kwh_year(kwh_year: float) -> float:
    return (kwh_year * 1000.0) / (365.0*24.0)

def split_eff(roundtrip):
    eta_c = eta_d = math.sqrt(max(min(roundtrip, 0.9999), 0.01))
    return eta_c, eta_d

def merge_gen():
    wind = read_series(DATA/"weather_wind.csv")
    solar = read_series(DATA/"solar_pv.csv")

    if wind is None and solar is None:
        raise SystemExit("No renewables found. Run scripts/fetch_weather.py and scripts/fetch_sun.py first.")

    frames = []
    if wind is not None:
        if "wind_kw" not in wind.columns: raise SystemExit("weather_wind.csv must have 'wind_kw'")
        frames.append(wind[["time","wind_kw"]])
    if solar is not None:
        if "pv_kw" not in solar.columns: raise SystemExit("solar_pv.csv must have 'pv_kw'")
        frames.append(solar[["time","pv_kw"]])

    df = frames[0]
    for f in frames[1:]:
        df = pd.merge_asof(df.sort_values("time"), f.sort_values("time"),
                           on="time", direction="nearest", tolerance=pd.Timedelta("30min"))
    df = df.fillna(0.0).sort_values("time").reset_index(drop=True)
    if "wind_kw" not in df.columns: df["wind_kw"]=0.0
    if "pv_kw" not in df.columns: df["pv_kw"]=0.0
    df["gen_kw"] = df["wind_kw"].astype(float) + df["pv_kw"].astype(float)
    df["dt_h"]   = infer_dt_hours(df["time"])
    return df

def preferred_windows():
    """Return a boolean Series of 'good time to run' (cheap or green) if we have data; else None."""
    price_path = DATA/"elspot_prices.csv"
    green_path = DATA/"green_schedule.csv"
    if not price_path.exists():
        return None

    prices = pd.read_csv(price_path, parse_dates=["time_dk"]).rename(columns={"time_dk":"time"}).sort_values("time")
    price_col = "price_dkk_per_kwh" if "price_dkk_per_kwh" in prices.columns else (
                "price_eur_per_kwh" if "price_eur_per_kwh" in prices.columns else None)
    if price_col is None:
        return None

    q = float(os.getenv("CHEAP_PRICE_QUANTILE", "0.35"))
    cheap_thr = prices[price_col].quantile(q)
    prices["cheap"] = prices[price_col] <= cheap_thr

    good = prices[["time","cheap"]].copy()

    # If we have composite "green" picks, mark those as good too
    if green_path.exists():
        g = pd.read_csv(green_path, parse_dates=["time"]).sort_values("time")
        g["green_pick"] = True
        good = pd.merge_asof(good.sort_values("time"),
                             g[["time","green_pick"]].sort_values("time"),
                             on="time", direction="nearest", tolerance=pd.Timedelta("15min"))
        good["good"] = good["cheap"].fillna(False) | good["green_pick"].fillna(False)
    else:
        good["good"] = good["cheap"]

    return good[["time","good"]]

def simulate(df, cap_kwh, init_soc_kwh, max_c_kw, max_d_kw, eta_rt,
             fridge_w=None, freezer_w=None):
    eta_c, eta_d = split_eff(eta_rt)
    dt_h = float(df["dt_h"].iloc[0])

    # background loads (W -> kW)
    if fridge_w is None:
        fridge_w = watts_from_kwh_year(float(os.getenv("FRIDGE_KWH_YEAR","150")))
    if freezer_w is None:
        freezer_w = watts_from_kwh_year(float(os.getenv("FREEZER_KWH_YEAR","200")))
    base_kw = (fridge_w + freezer_w) / 1000.0

    soc = float(init_soc_kwh)
    rows = []
    solar_in_kwh = 0.0
    wind_in_kwh  = 0.0
    for _, r in df.iterrows():
        t = r["time"]; wind_kw = float(r["wind_kw"]); pv_kw = float(r["pv_kw"])
        gen_kw = wind_kw + pv_kw
        net_kw = gen_kw - base_kw

        if net_kw > 0:
            room = cap_kwh - soc
            inflow = min(net_kw*dt_h*eta_c, max_c_kw*dt_h*eta_c, max(room,0.0))
            # apportion for stats
            share = (pv_kw/gen_kw) if gen_kw>1e-9 else 0.0
            solar_in_kwh += inflow*share
            wind_in_kwh  += inflow*(1.0-share)
            soc += inflow
        elif net_kw < 0:
            need = min((-net_kw)*dt_h/eta_d, max_d_kw*dt_h/eta_d)
            used = min(need, soc)
            soc -= used

        rows.append({"time":t, "soc_kwh":soc, "gen_kw":gen_kw, "base_kw":base_kw,
                     "wind_kw":wind_kw, "pv_kw":pv_kw})

    hist = pd.DataFrame(rows)
    total_wind_kwh  = (hist["wind_kw"]*dt_h).sum()
    total_solar_kwh = (hist["pv_kw"]*dt_h).sum()

    return hist, {
        "dt_minutes": int(dt_h*60),
        "capacity_kwh": cap_kwh,
        "soc_kwh_now": float(hist["soc_kwh"].iloc[-1]),
        "soc_pct_now": 100.0*float(hist["soc_kwh"].iloc[-1])/cap_kwh if cap_kwh>0 else 0.0,
        "base_kw": base_kw,
        "total_wind_kwh": total_wind_kwh,
        "total_solar_kwh": total_solar_kwh,
        "equiv_sun_hours": (total_solar_kwh / float(os.getenv("PV_KWP","5.0"))) if float(os.getenv("PV_KWP","5.0"))>0 else None,
    }

def cycles_possible_from_soc(soc_kwh, eta_d, power_kw, minutes_per_cycle):
    if power_kw <= 0 or minutes_per_cycle <= 0: return 0, 0.0
    e_cycle_kwh = power_kw * (minutes_per_cycle/60.0)
    # discharge losses: battery must provide e_cycle/eta_d
    needed_from_batt = e_cycle_kwh / eta_d
    max_cycles = int(soc_kwh // max(needed_from_batt, 1e-9))
    return max_cycles, max_cycles * minutes_per_cycle

def main():
    load_dotenv()

    # Inputs
    cap_kwh   = float(os.getenv("BATTERY_MAX_KWH","10"))
    init_soc  = float(os.getenv("BATTERY_INIT_SOC_KWH","0"))
    max_c_kw  = float(os.getenv("BATTERY_MAX_CHARGE_KW","5"))
    max_d_kw  = float(os.getenv("BATTERY_MAX_DISCHARGE_KW","5"))
    eta_rt    = float(os.getenv("BATTERY_EFFICIENCY","0.92"))
    reserve_h = float(os.getenv("COLD_RESERVE_HOURS","6"))

    # Appliance definitions
    appl = dict(
        toaster  = (float(os.getenv("TOASTER_KW","1.2")),   float(os.getenv("TOASTER_CYCLE_MIN","2"))),
        microwave= (float(os.getenv("MICROWAVE_KW","1.0")), float(os.getenv("MICROWAVE_CYCLE_MIN","5"))),
        airfryer = (float(os.getenv("AIRFRYER_KW","1.4")),  float(os.getenv("AIRFRYER_CYCLE_MIN","15"))),
        oven     = (float(os.getenv("OVEN_KW","2.2")),      float(os.getenv("OVEN_CYCLE_MIN","45"))),
    )

    df = merge_gen()
    hist, meta = simulate(df, cap_kwh, init_soc, max_c_kw, max_d_kw, eta_rt)

    # Efficiencies for discharge
    _, eta_d = split_eff(eta_rt)
    dt_h = meta["dt_minutes"]/60.0
    soc  = meta["soc_kwh_now"]
    base_kw = meta["base_kw"]

    # 1) Autonomy for fridge+freezer (no other loads)
    #    how many hours battery alone can supply base_kw (respecting discharge cap & efficiency)
    if base_kw <= 1e-9:
        autonomy_h = float("inf")
    else:
        # energy we can deliver per hour limited by discharge cap and efficiency
        max_deliverable_kw = min(base_kw, max_d_kw)  # power limit
        # energy drawn from battery to supply base_kw for 1h is base_kw/eta_d
        autonomy_h = soc / (base_kw/eta_d)

    # 2) Reserve to keep cold appliances alive for reserve_h
    reserve_kwh = (base_kw/eta_d) * reserve_h
    usable_surplus_kwh = max(0.0, soc - reserve_kwh)

    # 3) Per-appliance cycles from surplus (protecting the reserve)
    cycles = {}
    for name, (p_kw, mins) in appl.items():
        n, total_min = cycles_possible_from_soc(usable_surplus_kwh, eta_d, p_kw, mins)
        cycles[name] = (n, total_min, p_kw, mins)

    # 4) “Run now or wait?” suggestion
    #    Run now if (a) we have surplus and (b) current time is cheap/green (if we have that data).
    suggestion = "wait"
    if usable_surplus_kwh > 0.0:
        good = preferred_windows()
        if good is None:
            suggestion = "run now (no price/green data available)"
        else:
            # find the nearest “now” row
            now = hist["time"].iloc[-1]
            g_now = pd.merge_asof(pd.DataFrame({"time":[now]}), good, on="time",
                                  direction="nearest", tolerance=pd.Timedelta("15min"))
            if bool(g_now["good"].iloc[0]):
                suggestion = "run now (cheap/green window)"
            else:
                suggestion = "wait for cheap/green window"

    # 5) Forecast “time to full” from current SOC using the FUTURE rows (net positive only)
    soc_future = soc
    ttf_hours  = None
    eta_c, _ = split_eff(eta_rt)
    future = df.iloc[len(hist):].copy()
    if not future.empty and soc < cap_kwh-1e-9:
        for j, r in future.iterrows():
            net = float(r["gen_kw"] - base_kw)
            if net <= 0: continue
            inflow = min(net*dt_h*eta_c, max_c_kw*dt_h*eta_c, cap_kwh - soc_future)
            soc_future += inflow
            if soc_future >= cap_kwh-1e-9:
                ttf_hours = (j - future.index[0] + 1) * dt_h
                break

    # ---- Print summary ----
    print("\n=== Battery + Cold Appliances + Cycles ===")
    print(f"Step: {meta['dt_minutes']} min")
    print(f"Battery: {meta['capacity_kwh']:.2f} kWh | SOC: {soc:.2f} kWh ({meta['soc_pct_now']:.1f}%)")
    print(f"Base load (fridge+freezer): ~{base_kw*1000:.0f} W")
    print(f"Autonomy supplying only cold appliances: ~{autonomy_h:.1f} hours")

    print(f"\nEnergy generated in window -> Wind: {meta['total_wind_kwh']:.1f} kWh | Solar: {meta['total_solar_kwh']:.1f} kWh")
    if meta["equiv_sun_hours"] is not None:
        print(f"Equivalent Sun Hours (PV): {meta['equiv_sun_hours']:.1f} h (PV_kWp from .env)")

    print(f"\nReserve policy: keep {os.getenv('COLD_RESERVE_HOURS','6')} h for cold appliances "
          f"(~{reserve_kwh:.2f} kWh). Usable surplus now: {usable_surplus_kwh:.2f} kWh")

    print("\n>>> Cycles you can run NOW from surplus (protecting reserve):")
    for name, (n, total_min, p_kw, mins) in cycles.items():
        print(f"  {name:<9}  {n:4d} cycles  (~{total_min:5.1f} min)  — {p_kw:.1f} kW × {mins:.0f} min")

    if ttf_hours is not None:
        print(f"\nForecasted time to FULL (while covering cold appliances): ~{ttf_hours:.1f} h")
    else:
        print("\nForecasted time to FULL: not enough net generation in forecast window.")

    print(f"\nScheduling advice: {suggestion}")

    # Save SOC timeline
    out_soc = DATA/"battery_soc_from_renewables.csv"
    hist[["time","soc_kwh","gen_kw","base_kw","wind_kw","pv_kw"]].to_csv(out_soc, index=False)
    print(f"\nWrote SOC timeline: {out_soc}")

if __name__ == "__main__":
    main()
