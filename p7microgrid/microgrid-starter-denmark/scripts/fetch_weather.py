import os, json, math, requests, pandas as pd, numpy as np
from pathlib import Path
from dotenv import load_dotenv
from datetime import datetime, timezone, timedelta

OUT_CSV  = Path("data/weather_wind.csv")
OUT_JSON = Path("data/weather_wind.json")
OPEN_METEO = "https://api.open-meteo.com/v1/forecast"

def turbine_kw(v, rated_kw, v_rated, v_cutin, v_cutout):
    if v < v_cutin or v >= v_cutout: return 0.0
    if v >= v_rated: return rated_kw
    frac = (v - v_cutin) / max(v_rated - v_cutin, 1e-6)
    return rated_kw * max(0.0, min(1.0, frac**3))

def parse_dmi_coveragejson(js: dict, param_name: str) -> pd.DataFrame:
    """Parse DMI EDR CoverageJSON (position query)."""
    # CoverageJSON layout: domain.axes.t.values -> ISO times; ranges.<param>.values -> numeric series
    domain = js.get("domain", {})
    axes = domain.get("axes", {})
    t_vals = axes.get("t", {}).get("values", [])
    rngs = js.get("ranges", {})
    # Pick the first matching key (param or param variants)
    key = None
    for k in [param_name, param_name.replace("-10m",""), "wind-speed", "wind-speed-10m"]:
        if k in rngs:
            key = k; break
    if key is None:
        raise ValueError(f"Parameter '{param_name}' not found in CoverageJSON ranges: {list(rngs.keys())}")
    v = rngs[key].get("values", [])
    if len(t_vals) != len(v):
        # Sometimes CoverageJSON packs as pairs; try flatten
        if isinstance(v, list) and all(isinstance(x, list) for x in v):
            v = [x[0] for x in v]
    times = pd.to_datetime(t_vals, utc=True, errors="coerce")
    df = pd.DataFrame({"time": times, "wind_ms": pd.to_numeric(v, errors="coerce")})
    # Convert to DK local, drop tz (rest of your code expects naive local)
    df["time"] = df["time"].dt.tz_convert("Europe/Copenhagen").dt.tz_localize(None)
    return df.dropna().sort_values("time").reset_index(drop=True)

def fetch_dmi_edr(lat, lon, start_utc, end_utc) -> pd.DataFrame:
    api_key    = os.getenv("DMI_API_KEY", "").strip()
    collection = os.getenv("DMI_COLLECTION", "harmonie_dini_sf")
    param      = os.getenv("DMI_PARAM", "wind-speed-10m")
    if not api_key:
        raise RuntimeError("DMI_API_KEY missing")

    # DMI wants lon lat ordering and CRS84
    coords = f"POINT({lon} {lat})"
    # CoverageJSON works for point series; GeoJSON is universal but larger
    url = (
        f"https://dmigw.govcloud.dk/v1/forecastedr/collections/{collection}/position"
        f"?coords={coords}&crs=crs84&parameter-name={param}"
        f"&datetime={start_utc}/{end_utc}&f=CoverageJSON"
    )
    r = requests.get(url, headers={"api-key": api_key}, timeout=35)
    r.raise_for_status()
    js = r.json()
    return parse_dmi_coveragejson(js, param)

def fetch_open_meteo(lat, lon, hours=168, tz="Europe/Copenhagen") -> pd.DataFrame:
    past_days = min(7, max(0, hours // 24))
    forecast_days = min(10, max(1, hours // 24))
    params = {
        "latitude": lat, "longitude": lon,
        "hourly": "wind_speed_10m",
        "windspeed_unit": "ms",
        "past_days": past_days,
        "forecast_days": forecast_days,
        "timezone": tz,
    }
    r = requests.get(OPEN_METEO, params=params, timeout=30)
    r.raise_for_status()
    h = r.json().get("hourly", {})
    times = pd.to_datetime(h.get("time", []))
    df = pd.DataFrame({"time": times})
    df["time"] = df["time"]  # already in local tz if timezone param was used
    df["wind_ms"] = pd.to_numeric(h.get("wind_speed_10m", [np.nan]*len(df)), errors="coerce")
    return df.dropna().sort_values("time").reset_index(drop=True)

def add_kw(df: pd.DataFrame) -> pd.DataFrame:
    rated_kw = float(os.getenv("TURBINE_RATED_KW", "3.0"))
    v_rated  = float(os.getenv("TURBINE_RATED_MS", "12.0"))
    v_cutin  = float(os.getenv("TURBINE_CUTIN_MS", "3.0"))
    v_cutout = float(os.getenv("TURBINE_CUTOUT_MS", "25.0"))
    df["wind_kw"] = [turbine_kw(v, rated_kw, v_rated, v_cutin, v_cutout)
                     for v in df["wind_ms"].fillna(0.0)]
    return df

if __name__ == "__main__":
    load_dotenv()
    Path("data").mkdir(exist_ok=True)

    lat = float(os.getenv("LAT", "57.0488"))
    lon = float(os.getenv("LON", "9.9217"))
    past_h = int(os.getenv("WEATHER_PAST_HOURS", "24"))
    fwd_h  = int(os.getenv("WEATHER_FWD_HOURS", "120"))

    now = datetime.now(timezone.utc)
    start_utc = (now - timedelta(hours=past_h)).isoformat().replace("+00:00","Z")
    end_utc   = (now + timedelta(hours=fwd_h)).isoformat().replace("+00:00","Z")

    # Try DMI first; fallback to Open-Meteo
    try:
        if os.getenv("DMI_API_KEY", "").strip():
            df = fetch_dmi_edr(lat, lon, start_utc, end_utc)
        else:
            raise RuntimeError("No DMI key")
    except Exception as e:
        print(f"[DMI] {e} -> falling back to Open-Meteo")
        df = fetch_open_meteo(lat, lon, hours=past_h + fwd_h)

    df = add_kw(df).drop_duplicates(subset=["time"]).sort_values("time").reset_index(drop=True)
    df.to_csv(OUT_CSV, index=False)
    with open(OUT_JSON, "w", encoding="utf-8") as f:
        json.dump(
            [{"time": t.isoformat(), "wind_ms": float(ms), "wind_kw": float(kw)}
             for t, ms, kw in zip(df["time"], df["wind_ms"], df["wind_kw"])],
            f, ensure_ascii=False, indent=2
        )

    print(f"Wrote {OUT_CSV} & {OUT_JSON}  ({len(df)} rows)")
    print(f"Range: {df['time'].min()} → {df['time'].max()}")
