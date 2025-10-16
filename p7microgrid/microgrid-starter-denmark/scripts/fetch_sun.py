import os
import math
import requests
import pandas as pd
from dotenv import load_dotenv

OPEN_METEO = "https://api.open-meteo.com/v1/forecast"

def fetch_solar_series(lat, lon, tz="Europe/Copenhagen"):
    """
    Try to fetch POA irradiance directly; if not available, fall back to shortwave (GHI).
    Returns a DataFrame with columns: time (datetime64[ns]), poa_wm2 (float), temp_c (float)
    """
    # Ask for several solar vars; API returns what's available
    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": ",".join([
            "global_tilted_irradiance",   # W/m2 on tilted plane (POA). Best-case.
            "shortwave_radiation",        # GHI W/m2 (fallback)
            "temperature_2m",             # ambient °C
            "direct_radiation",           # optional, may help later
            "diffuse_radiation"
        ]),
        "past_days": 2,                   # recent history
        "forecast_days": 7,               # near-term forecast
        "timezone": tz,
    }
    r = requests.get(OPEN_METEO, params=params, timeout=30)
    r.raise_for_status()
    js = r.json()
    hourly = js.get("hourly", {})
    times = pd.to_datetime(hourly.get("time", []))
    if times.empty:
        raise RuntimeError("Open-Meteo returned no hourly times.")

    df = pd.DataFrame({"time": times})
    def add_col(key, name):
        if key in hourly:
            df[name] = pd.to_numeric(hourly[key], errors="coerce")
    add_col("global_tilted_irradiance", "poa_wm2")
    add_col("shortwave_radiation", "ghi_wm2")
    add_col("temperature_2m", "temp_c")

    # If no POA, use GHI as proxy (underestimates at non-noon; okay as a fallback)
    if "poa_wm2" not in df:
        if "ghi_wm2" not in df:
            raise RuntimeError("Neither POA nor GHI available from Open-Meteo.")
        df["poa_wm2"] = df["ghi_wm2"].clip(lower=0.0)

    # Ensure temperature present
    if "temp_c" not in df:
        df["temp_c"] = float(os.getenv("DEFAULT_TEMP_C", "15.0"))

    return df[["time", "poa_wm2", "temp_c"]].sort_values("time").reset_index(drop=True)

def pv_power_kw(poa_wm2, temp_c, kWp, noct=45.0, gamma_p=-0.004, inv_eff=0.96):
    """
    Very common first-order PV model:
      - Cell temperature: Tcell ≈ Tamb + (NOCT-20)/800 * POA
      - Power derate:     Pdc = kWp * (POA/1000) * (1 + gamma_p * (Tcell - 25))
      - AC power:         Pac = Pdc * inv_eff
      - clipped to [0, kWp * inv_eff]
    """
    t_cell = temp_c + (noct - 20.0) / 800.0 * poa_wm2
    p_dc = kWp * (poa_wm2 / 1000.0) * (1.0 + gamma_p * (t_cell - 25.0))
    p_dc = max(0.0, p_dc)
    p_ac = p_dc * inv_eff
    return max(0.0, min(p_ac, kWp * inv_eff))

if __name__ == "__main__":
    load_dotenv()
    lat = float(os.getenv("LAT", "57.0488"))
    lon = float(os.getenv("LON", "9.9217"))
    kWp = float(os.getenv("PV_KWP", "5.0"))
    noct = float(os.getenv("PV_NOCT", "45"))
    gamma_p = float(os.getenv("PV_GAMMA_P", "-0.004"))
    inv_eff = float(os.getenv("PV_INVERTER_EFF", "0.96"))

    df = fetch_solar_series(lat, lon, tz="Europe/Copenhagen")
    df["pv_kw"] = [
        pv_power_kw(poa, t, kWp, noct=noct, gamma_p=gamma_p, inv_eff=inv_eff)
        for poa, t in zip(df["poa_wm2"].fillna(0.0), df["temp_c"].fillna(15.0))
    ]

    out = "data/solar_pv.csv"
    df.to_csv(out, index=False)
    print(f"Wrote {out} ({len(df)} rows)")
