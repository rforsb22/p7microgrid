# scripts/fetch_weather_history.py
"""
Fetch hourly historical wind observations from DMI metObs and convert to turbine power.

Writes:
  - data/weather_wind.csv
  - data/weather_wind.json

Env:
  DMI_METOBS_BASE=https://dmigw.govcloud.dk/v2/metObs
  DMI_API_KEY=YOUR_KEY            # metObs v2 key (header X-Gravitee-Api-Key or ?api-key=)
  DMI_PARAM_ID=wind_speed_past1h  # hourly 10 m mean wind (m/s)
  DMI_STATION_ID=                 # optional; if empty we pick nearest active station to LAT/LON

  LAT=57.0488
  LON=9.9217

  # time window (UTC)
  HIST_START=2025-10-01T00:00:00Z
  HIST_END=2025-10-22T00:00:00Z

  # turbine curve
  TURBINE_RATED_KW=3.0
  TURBINE_RATED_MS=12.0
  TURBINE_CUTIN_MS=3.0
  TURBINE_CUTOUT_MS=25.0

  # Optional
  PRINT_URL=0
"""

import os, math, json
from pathlib import Path
from datetime import datetime, timezone
from typing import Optional, Dict, List

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from zoneinfo import ZoneInfo

EU_CPH = ZoneInfo("Europe/Copenhagen")
OUT_CSV  = Path("data/weather_wind.csv")
OUT_JSON = Path("data/weather_wind.json")

def _session() -> requests.Session:
    retry = Retry(total=5, connect=5, read=5, backoff_factor=0.8,
                  status_forcelist=[429, 500, 502, 503, 504], allowed_methods=["GET"],
                  raise_on_status=False)
    s = requests.Session()
    a = HTTPAdapter(max_retries=retry, pool_connections=10, pool_maxsize=10)
    s.mount("https://", a); s.mount("http://", a)
    return s

def _haversine_km(lat1, lon1, lat2, lon2) -> float:
    R = 6371.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlmb = math.radians(lon2 - lon1)
    a = math.sin(dphi/2)**2 + math.cos(p1)*math.cos(p2)*math.sin(dlmb/2)**2
    return 2 * R * math.asin(math.sqrt(a))

def _pick_nearest_station(base: str, apikey: str, lat: float, lon: float) -> str:
    """
    Query stations near (lat, lon), expanding a bbox until at least one 'Active' station is found.
    """
    sess = _session()
    headers = {"X-Gravitee-Api-Key": apikey} if apikey else {}
    # start with ~20km box in degrees; expand if needed
    for half_deg in [0.2, 0.4, 0.8, 1.2, 2.0]:
        bbox = f"{lon-half_deg},{lat-half_deg},{lon+half_deg},{lat+half_deg}"
        url = f"{base}/collections/station/items"
        params = {"bbox": bbox, "status": "Active", "limit": 1000}
        r = sess.get(url, params=params, headers=headers, timeout=(10, 60))
        if r.status_code >= 400:
            raise RuntimeError(f"metObs station query {r.status_code} {r.url}\n{r.text[:400]}")
        items = r.json().get("features", [])
        if items:
            # choose nearest
            items = [
                (feat["properties"]["stationId"],
                 feat["geometry"]["coordinates"][1],  # lat
                 feat["geometry"]["coordinates"][0])  # lon
                for feat in items if "geometry" in feat and feat["geometry"]
            ]
            best = min(items, key=lambda it: _haversine_km(lat, lon, it[1], it[2]))
            return best[0]
    raise RuntimeError("No active stations found near the provided location.")

def _list_observations(base: str, apikey: str, station: str, param_id: str,
                       start_iso: str, end_iso: str, print_url: bool) -> List[Dict]:
    """
    Page through /observation/items (limit/offset) and collect observations.
    NOTE: Do NOT use 'sortorder' — some deployments reject it. We sort locally instead.
    """
    sess = _session()
    headers = {"X-Gravitee-Api-Key": apikey} if apikey else {}
    url = f"{base}/collections/observation/items"
    limit, offset = 10000, 0
    out: List[Dict] = []

    while True:
        params = {
            "stationId": station,
            "parameterId": param_id,
            "datetime": f"{start_iso}/{end_iso}",
            "limit": limit,
            "offset": offset,
            # "sortorder": "observed,ASC",  # <-- removed, we’ll sort locally
        }
        if print_url:
            prep = requests.Request("GET", url, params=params, headers=headers).prepare()
            print("metObs GET:", prep.url)

        r = sess.get(url, params=params, headers=headers, timeout=(10, 90))
        if r.status_code >= 400:
            raise RuntimeError(f"metObs {r.status_code} {r.url}\n{r.text[:500]}")

        js = r.json()
        feats = js.get("features", [])
        if not feats:
            break

        for f in feats:
            props = f.get("properties", {})
            val = props.get("value")
            obs_time = props.get("observed")
            if val is None or obs_time is None:
                continue
            out.append({"observed": obs_time, "value": float(val)})

        n = len(feats)
        if n < limit:
            break
        offset += n

    # Local sort by observation time (ascending)
    out.sort(key=lambda x: x["observed"])
    return out


def turbine_kw(v: float, rated_kw: float, v_rated: float, v_cutin: float, v_cutout: float) -> float:
    if v < v_cutin or v >= v_cutout: return 0.0
    if v >= v_rated: return rated_kw
    frac = (v - v_cutin) / max(v_rated - v_cutin, 1e-6)
    return rated_kw * max(0.0, min(1.0, frac**3))

def main():
    from dotenv import load_dotenv
    load_dotenv()
    Path("data").mkdir(exist_ok=True)

    base = os.getenv("DMI_METOBS_BASE", "https://dmigw.govcloud.dk/v2/metObs").strip()
    apikey = os.getenv("DMI_API_KEY_HISTORY", "").strip()
    param_id = os.getenv("DMI_PARAM_ID", "wind_speed_past1h").strip()
    station = os.getenv("DMI_STATION_ID", "").strip()
    lat = float(os.getenv("LAT", "57.0488")); lon = float(os.getenv("LON", "9.9217"))
    start_iso = os.getenv("HIST_START")
    end_iso   = os.getenv("HIST_END")
    if not (start_iso and end_iso):
        # default: last 7 days
        now = datetime.now(timezone.utc)
        end_iso = now.isoformat().replace("+00:00", "Z")
        start_iso = (now - pd.Timedelta(days=7)).isoformat().replace("+00:00", "Z")

    print_url = os.getenv("PRINT_URL", "0").strip() == "1"

    if not apikey:
        raise RuntimeError("DMI_API_KEY (metObs) missing")

    if not station:
        station = _pick_nearest_station(base, apikey, lat, lon)
        print(f"Using nearest active station: {station}")

    obs = _list_observations(base, apikey, station, param_id, start_iso, end_iso, print_url)
    if not obs:
        raise RuntimeError("No observations returned.")

    # Build dataframe: metObs times are UTC ISO
    df = pd.DataFrame(obs)
    df["time_utc"] = pd.to_datetime(df["observed"], utc=True, errors="coerce")
    df = df.dropna().sort_values("time_utc").reset_index(drop=True)

    # Convert to Europe/Copenhagen *local naive* to match your pipeline
    df["time"] = df["time_utc"].dt.tz_convert(EU_CPH).dt.tz_localize(None)
    df["wind_ms"] = df["value"].astype(float)

    rated_kw = float(os.getenv("TURBINE_RATED_KW", "3.0"))
    v_rated  = float(os.getenv("TURBINE_RATED_MS", "12.0"))
    v_cutin  = float(os.getenv("TURBINE_CUTIN_MS", "3.0"))
    v_cutout = float(os.getenv("TURBINE_CUTOUT_MS", "25.0"))
    df["wind_kw"] = [
        turbine_kw(v, rated_kw, v_rated, v_cutin, v_cutout)
        for v in df["wind_ms"].fillna(0.0)
    ]

    # Keep only the three columns your services consume
    out = df[["time", "wind_ms", "wind_kw"]].drop_duplicates(subset=["time"]).sort_values("time")

    # Write
    out.to_csv(OUT_CSV, index=False)
    with OUT_JSON.open("w", encoding="utf-8") as f:
        json.dump(
            [{"time": t.isoformat(), "wind_ms": float(ms), "wind_kw": float(kw)}
             for t, ms, kw in out.itertuples(index=False, name=None)],
            f, ensure_ascii=False, indent=2
        )

    print(f"Wrote {OUT_CSV} & {OUT_JSON}  ({len(out)} rows)")
    print(f"Range: {out['time'].min()} → {out['time'].max()}")

if __name__ == "__main__":
    main()
