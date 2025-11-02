# api/services/wind_forecast.py
from __future__ import annotations
import os
from datetime import datetime, timedelta, timezone
from typing import List, Dict
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from zoneinfo import ZoneInfo
from dotenv import load_dotenv

load_dotenv()

EU_CPH = ZoneInfo("Europe/Copenhagen")

# --- DMI Forecast EDR config ---
DMI_EDR_BASE = os.getenv("DMI_EDR_BASE", "https://dmigw.govcloud.dk/v1/forecastedr").rstrip("/")
DMI_EDR_COLLECTION = os.getenv("DMI_EDR_COLLECTION", "harmonie_dini_sf")
DMI_EDR_PARAM = os.getenv("DMI_EDR_PARAM", "wind-speed-10m")  # m/s
DMI_API_KEY_EDR = os.getenv("DMI_API_KEY_EDR", "").strip()

# --- Turbine model (same semantics as your other services) ---
TURBINE_RATED_KW = float(os.getenv("TURBINE_RATED_KW", "3.0"))
TURBINE_RATED_MS = float(os.getenv("TURBINE_RATED_MS", "12.0"))
TURBINE_CUTIN_MS  = float(os.getenv("TURBINE_CUTIN_MS", "3.0"))
TURBINE_CUTOUT_MS = float(os.getenv("TURBINE_CUTOUT_MS", "25.0"))

def _session() -> requests.Session:
    retry = Retry(
        total=5, connect=5, read=5, backoff_factor=0.6,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    s = requests.Session()
    a = HTTPAdapter(max_retries=retry, pool_connections=10, pool_maxsize=10)
    s.mount("https://", a); s.mount("http://", a)
    return s

def _headers():
    return {"X-Gravitee-Api-Key": DMI_API_KEY_EDR} if DMI_API_KEY_EDR else {}

def _turbine_kw(v_ms: float) -> float:
    if v_ms < TURBINE_CUTIN_MS or v_ms >= TURBINE_CUTOUT_MS:
        return 0.0
    if v_ms >= TURBINE_RATED_MS:
        return TURBINE_RATED_KW
    frac = (v_ms - TURBINE_CUTIN_MS) / max(TURBINE_RATED_MS - TURBINE_CUTIN_MS, 1e-6)
    return TURBINE_RATED_KW * max(0.0, min(1.0, frac**3))

def forecast_wind_power(lat: float, lon: float, hours: int = 48) -> List[Dict[str, object]]:
    """
    Live wind forecast from DMI Forecast EDR (HARMONIE DINI SF).
    Returns next N hours as [{ts (UTC), wind_ms, wind_kw}] in m/s.
    """
    base = f"{DMI_EDR_BASE}/collections/{DMI_EDR_COLLECTION}/position"
    now_utc = datetime.now(timezone.utc)
    end_utc = now_utc + timedelta(hours=hours)

    params = {
        # EDR position expects POINT(lon lat)
        "coords": f"POINT({lon:.5f} {lat:.5f})",
        "parameter-name": DMI_EDR_PARAM,  # e.g. "wind-speed-10m" (m/s)
        "datetime": f"{now_utc.isoformat(timespec='seconds').replace('+00:00','Z')}/"
                    f"{end_utc.isoformat(timespec='seconds').replace('+00:00','Z')}",
        "crs": "crs84",
        "f": "CoverageJSON",
    }

    sess = _session()
    r = sess.get(base, params=params, headers=_headers(), timeout=45)
    if r.status_code >= 400:
        raise RuntimeError(f"DMI EDR {r.status_code} {r.url}\n{r.text[:600]}")

    js = r.json()
    # CoverageJSON layout: domain.axes.t.values (ISO times), ranges.<param>.values (numbers)
    domain = js.get("domain", {})
    axes = domain.get("axes", {})
    tvals = axes.get("t", {}).get("values", []) or []
    ranges = js.get("ranges", {}) or {}

    if len(ranges) != 1:
        raise RuntimeError("Unexpected CoverageJSON 'ranges' structure from DMI EDR.")
    param_key = next(iter(ranges.keys()))
    values = ranges[param_key].get("values", []) or []

    out: List[Dict[str, object]] = []
    for t_iso, v in zip(tvals, values):
        ts = datetime.fromisoformat(t_iso.replace("Z", "+00:00")).astimezone(timezone.utc)
        v_ms = max(0.0, float(v) if v is not None else 0.0)  # units already m/s
        out.append({"ts": ts, "wind_ms": v_ms, "wind_kw": _turbine_kw(v_ms)})
    out.sort(key=lambda r: r["ts"])

    # Clip in case the model returns more than requested
    return [r for r in out if now_utc <= r["ts"] <= end_utc]
