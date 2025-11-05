# api/services/wind_live.py
from __future__ import annotations
import os, math
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Optional
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from zoneinfo import ZoneInfo

from dotenv import load_dotenv
load_dotenv()

EU_CPH = ZoneInfo("Europe/Copenhagen")

DMI_BASE = os.getenv("DMI_METOBS_BASE", "https://dmigw.govcloud.dk/v2/metObs").rstrip("/")
DMI_API_KEY = os.getenv("DMI_API_KEY_METOBS", os.getenv("DMI_API_KEY_EDR", "")).strip()
DMI_PARAM_ID = os.getenv("DMI_PARAM_ID", "wind_speed_past1h").strip()

# Turbine params (same semantics as your script)
TURBINE_RATED_KW = float(os.getenv("TURBINE_RATED_KW", "3.0"))
TURBINE_RATED_MS = float(os.getenv("TURBINE_RATED_MS", "12.0"))
TURBINE_CUTIN_MS  = float(os.getenv("TURBINE_CUTIN_MS", "3.0"))
TURBINE_CUTOUT_MS = float(os.getenv("TURBINE_CUTOUT_MS", "25.0"))

def _session() -> requests.Session:
    retry = Retry(total=5, connect=5, read=5, backoff_factor=0.8,
                  status_forcelist=[429, 500, 502, 503, 504], allowed_methods=["GET"],
                  raise_on_status=False)
    s = requests.Session()
    a = HTTPAdapter(max_retries=retry, pool_connections=10, pool_maxsize=10)
    s.mount("https://", a); s.mount("http://", a)
    return s

def _hav_km(lat1, lon1, lat2, lon2) -> float:
    R = 6371.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlmb = math.radians(lon2 - lon1)
    a = math.sin(dphi/2)**2 + math.cos(p1)*math.cos(p2)*math.sin(dlmb/2)**2
    return 2 * R * math.asin(math.sqrt(a))

def _pick_nearest_station(lat: float, lon: float) -> str:
    """Find nearest 'Active' station using bbox expansion."""
    sess = _session()
    headers = {"X-Gravitee-Api-Key": DMI_API_KEY} if DMI_API_KEY else {}
    url = f"{DMI_BASE}/collections/station/items"
    for half_deg in [0.2, 0.4, 0.8, 1.2, 2.0]:
        bbox = f"{lon-half_deg},{lat-half_deg},{lon+half_deg},{lat+half_deg}"
        r = sess.get(url, params={"bbox": bbox, "status": "Active", "limit": 1000},
                     headers=headers, timeout=(10,60))
        if r.status_code >= 400:
            raise RuntimeError(f"DMI stations {r.status_code} {r.url}\n{r.text[:400]}")
        feats = r.json().get("features", [])
        if feats:
            items = []
            for f in feats:
                if "geometry" in f and f["geometry"]:
                    sid = f["properties"]["stationId"]
                    slat = f["geometry"]["coordinates"][1]
                    slon = f["geometry"]["coordinates"][0]
                    items.append((sid, slat, slon))
            if items:
                best = min(items, key=lambda it: _hav_km(lat, lon, it[1], it[2]))
                return best[0]
    raise RuntimeError("No active DMI stations found near the provided location.")

def _list_wind(station_id: str, start_iso: str, end_iso: str) -> List[Dict[str, object]]:
    """List wind observations via /collections/observation/items (paged)."""
    sess = _session()
    headers = {"X-Gravitee-Api-Key": DMI_API_KEY} if DMI_API_KEY else {}
    url = f"{DMI_BASE}/collections/observation/items"
    limit, offset = 10000, 0
    rows: List[Dict[str, object]] = []
    while True:
        params = {
            "stationId": station_id,
            "parameterId": DMI_PARAM_ID,      # wind_speed_past1h
            "datetime": f"{start_iso}/{end_iso}",
            "limit": limit,
            "offset": offset,
        }
        r = sess.get(url, params=params, headers=headers, timeout=(10,90))
        if r.status_code >= 400:
            raise RuntimeError(f"DMI obs {r.status_code} {r.url}\n{r.text[:500]}")
        feats = r.json().get("features", [])
        if not feats:
            break
        for f in feats:
            props = f.get("properties", {})
            v = props.get("value")
            t = props.get("observed")
            if v is None or t is None:
                continue
            # observed is UTC time; treat as such
            ts = datetime.fromisoformat(t.replace("Z", "+00:00")).astimezone(timezone.utc)
            rows.append({"ts": ts, "wind_ms": float(v)})
        if len(feats) < limit:
            break
        offset += len(feats)
    rows.sort(key=lambda x: x["ts"])
    return rows

def turbine_kw(v: float) -> float:
    if v < TURBINE_CUTIN_MS or v >= TURBINE_CUTOUT_MS:
        return 0.0
    if v >= TURBINE_RATED_MS:
        return TURBINE_RATED_KW
    frac = (v - TURBINE_CUTIN_MS) / max(TURBINE_RATED_MS - TURBINE_CUTIN_MS, 1e-6)
    return TURBINE_RATED_KW * max(0.0, min(1.0, frac**3))

def historic_wind_power(
    lat: float,
    lon: float,
    station_id: Optional[str],
    start_utc: datetime,
    end_utc: datetime,
) -> List[Dict[str, object]]:
    """Return [{ts, wind_ms, wind_kw}] from DMI metObs for [start,end]."""
    sid = station_id or _pick_nearest_station(lat, lon)
    start_iso = start_utc.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    end_iso   = end_utc.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    rows = _list_wind(sid, start_iso, end_iso)
    out = []
    for r in rows:
        out.append({"ts": r["ts"], "wind_ms": r["wind_ms"], "wind_kw": turbine_kw(r["wind_ms"])})
    return out

def current_wind_power(lat: float, lon: float, station_id: Optional[str]) -> Dict[str, object]:
    """Return latest available sample as 'current'."""
    now = datetime.now(timezone.utc)
    # pull last 6 hours to be safe
    rows = historic_wind_power(lat, lon, station_id, now - timedelta(hours=6), now)
    if not rows:
        return {"available": False, "reason": "no_observations"}
    last = rows[-1]
    return {
        "available": True,
        "now_utc": last["ts"].isoformat(),
        "wind_ms": last["wind_ms"],
        "wind_kw": last["wind_kw"],
        "source": {"station_id": station_id or "nearest", "lookback_hours": 6},
    }
