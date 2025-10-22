"""
Fetch hourly wind forecast from DMI Forecast EDR and convert to turbine power.

Outputs:
  - data/weather_wind.csv
  - data/weather_wind.json

Env variables (example):
  DMI_BASE_URL=https://dmigw.govcloud.dk/v1/forecastedr
  DMI_COLLECTION=harmonie_dini_sf
  DMI_PARAM=wind-speed-10m
  DMI_API_KEY=YOUR_KEY

  LAT=57.0488
  LON=9.9217
  WEATHER_PAST_HOURS=24
  WEATHER_FWD_HOURS=120

  TURBINE_RATED_KW=3.0
  TURBINE_RATED_MS=12.0
  TURBINE_CUTIN_MS=3.0
  TURBINE_CUTOUT_MS=25.0

  # Optional debugging
  PRINT_URL=0
"""

import os
import json
from pathlib import Path
from datetime import datetime, timezone, timedelta

import numpy as np
import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from dotenv import load_dotenv


OUT_CSV = Path("data/weather_wind.csv")
OUT_JSON = Path("data/weather_wind.json")


def turbine_kw(v: float, rated_kw: float, v_rated: float, v_cutin: float, v_cutout: float) -> float:
    """Simple cubic power curve between cut-in and rated; 0 outside [cut-in, cut-out)."""
    if v < v_cutin or v >= v_cutout:
        return 0.0
    if v >= v_rated:
        return rated_kw
    frac = (v - v_cutin) / max(v_rated - v_cutin, 1e-6)
    return rated_kw * max(0.0, min(1.0, frac ** 3))


def parse_dmi_coveragejson(js: dict, param_name: str) -> pd.DataFrame:
    """
    Parse DMI EDR CoverageJSON (position query).
    domain.axes.t.values -> ISO times (UTC)
    ranges.<param>.values -> numeric series
    """
    domain = js.get("domain", {})
    axes = domain.get("axes", {})
    t_vals = axes.get("t", {}).get("values", [])
    rngs = js.get("ranges", {})

    # Try requested key and common aliases (hyphenated forms)
    candidates = [
        param_name,
        param_name.replace("_", "-"),
        "wind-speed-10m",
        "wind-speed",
    ]
    key = next((k for k in candidates if k in rngs), None)
    if key is None:
        raise ValueError(f"Parameter '{param_name}' not found in CoverageJSON ranges: {list(rngs.keys())}")

    v = rngs[key].get("values", [])
    # Sometimes values could be nested lists (e.g., [[x], [y], ...]) -> flatten first element
    if len(t_vals) != len(v) and isinstance(v, list) and all(isinstance(x, list) for x in v):
        v = [x[0] for x in v]

    times = pd.to_datetime(t_vals, utc=True, errors="coerce")
    df = pd.DataFrame({"time": times, "wind_ms": pd.to_numeric(v, errors="coerce")})

    # Convert to Europe/Copenhagen and drop tz to keep pipeline simple
    df["time"] = df["time"].dt.tz_convert("Europe/Copenhagen").dt.tz_localize(None)

    return df.dropna().sort_values("time").reset_index(drop=True)


def _requests_session() -> requests.Session:
    """Create a session with retry/backoff for transient gateway slowness."""
    retry = Retry(
        total=5,
        connect=5,
        read=5,
        backoff_factor=0.8,  # 0.8, 1.6, 3.2, 6.4, ...
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=10, pool_maxsize=10)
    s = requests.Session()
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s


def _iter_time_chunks(start_iso: str, end_iso: str, chunk_hours: int = 48):
    """Yield [start, end] ISO Z pairs split into chunk_hours windows."""
    start = pd.to_datetime(start_iso)
    end = pd.to_datetime(end_iso)
    cur = start
    delta = timedelta(hours=chunk_hours)
    while cur < end:
        nxt = min(cur + delta, end)
        yield (
            cur.strftime("%Y-%m-%dT%H:%M:%SZ"),
            nxt.strftime("%Y-%m-%dT%H:%M:%SZ"),
        )
        cur = nxt


def fetch_dmi_edr(lat: float, lon: float, start_utc: str, end_utc: str) -> pd.DataFrame:
    """
    Query DMI Forecast EDR for a single point (lon/lat) over a given time window.
    Uses:
      - correct header 'X-Gravitee-Api-Key'
      - hyphenated parameter names
      - retries + longer timeouts
      - 48-hour chunking to keep responses light and avoid gateway timeouts
    """
    api_key = os.getenv("DMI_API_KEY", "").strip()
    collection = os.getenv("DMI_COLLECTION", "harmonie_dini_sf").strip()
    # DMI parameters are hyphenated, e.g., wind-speed or wind-speed-10m
    param = os.getenv("DMI_PARAM", "wind-speed-10m").strip()
    base = os.getenv("DMI_BASE_URL", "https://dmigw.govcloud.dk/v1/forecastedr").strip()
    print_url = os.getenv("PRINT_URL", "0").strip() == "1"

    if not api_key:
        raise RuntimeError("DMI_API_KEY missing")

    url = f"{base}/collections/{collection}/position"
    headers = {"X-Gravitee-Api-Key": api_key}
    timeout = (10, 120)  # (connect, read) seconds

    session = _requests_session()
    frames: list[pd.DataFrame] = []

    for s_iso, e_iso in _iter_time_chunks(start_utc, end_utc, chunk_hours=48):
        params = {
            "coords": f"POINT({lon} {lat})",  # NOTE: 'POINT(lon lat)' and crs84
            "crs": "crs84",
            "parameter-name": param,
            "datetime": f"{s_iso}/{e_iso}",
            "f": "CoverageJSON",
        }
        if print_url:
            # Build the prepared URL for visibility
            prep = requests.Request("GET", url, params=params, headers=headers).prepare()
            print("DMI GET:", prep.url)

        r = session.get(url, params=params, headers=headers, timeout=timeout)
        if r.status_code >= 400:
            raise RuntimeError(f"DMI {r.status_code} at {r.url}\nBody: {r.text[:500]}")
        frames.append(parse_dmi_coveragejson(r.json(), param))

    if not frames:
        raise RuntimeError("No data returned from DMI EDR")

    df = pd.concat(frames, ignore_index=True)
    # Drop duplicates across chunk boundaries, keep earliest
    df = df.drop_duplicates(subset=["time"]).sort_values("time").reset_index(drop=True)
    return df


def add_kw(df: pd.DataFrame) -> pd.DataFrame:
    rated_kw = float(os.getenv("TURBINE_RATED_KW", "3.0"))
    v_rated = float(os.getenv("TURBINE_RATED_MS", "12.0"))
    v_cutin = float(os.getenv("TURBINE_CUTIN_MS", "3.0"))
    v_cutout = float(os.getenv("TURBINE_CUTOUT_MS", "25.0"))
    df["wind_kw"] = [
        turbine_kw(v, rated_kw, v_rated, v_cutin, v_cutout)
        for v in df["wind_ms"].fillna(0.0)
    ]
    return df


if __name__ == "__main__":
    load_dotenv()
    Path("data").mkdir(exist_ok=True)

    lat = float(os.getenv("LAT", "57.0488"))
    lon = float(os.getenv("LON", "9.9217"))
    past_h = int(os.getenv("WEATHER_PAST_HOURS", "24"))
    fwd_h = int(os.getenv("WEATHER_FWD_HOURS", "120"))

    now = datetime.now(timezone.utc)
    start_utc = (now - timedelta(hours=past_h)).isoformat().replace("+00:00", "Z")
    end_utc = (now + timedelta(hours=fwd_h)).isoformat().replace("+00:00", "Z")

    # DMI only
    df = fetch_dmi_edr(lat, lon, start_utc, end_utc)
    df = (
        add_kw(df)
        .drop_duplicates(subset=["time"])
        .sort_values("time")
        .reset_index(drop=True)
    )

    df.to_csv(OUT_CSV, index=False)
    with open(OUT_JSON, "w", encoding="utf-8") as f:
        json.dump(
            [
                {"time": t.isoformat(), "wind_ms": float(ms), "wind_kw": float(kw)}
                for t, ms, kw in zip(df["time"], df["wind_ms"], df["wind_kw"])
            ],
            f,
            ensure_ascii=False,
            indent=2,
        )

    print(f"Wrote {OUT_CSV} & {OUT_JSON}  ({len(df)} rows)")
    print(f"Range: {df['time'].min()} → {df['time'].max()}")
