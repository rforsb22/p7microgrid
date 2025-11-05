# api/services/solar_pvgis.py
from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import List, Dict
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from zoneinfo import ZoneInfo
from dotenv import load_dotenv

load_dotenv()

EU_CPH = ZoneInfo("Europe/Copenhagen")
PVGIS_BASE = os.getenv("PVGIS_BASE", "https://re.jrc.ec.europa.eu/api/v5_3/seriescalc")

def _session() -> requests.Session:
    retry = Retry(total=5, connect=5, read=5, backoff_factor=0.8,
                  status_forcelist=[429, 500, 502, 503, 504], allowed_methods=["GET"],
                  raise_on_status=False)
    s = requests.Session()
    a = HTTPAdapter(max_retries=retry, pool_connections=10, pool_maxsize=10)
    s.mount("https://", a); s.mount("http://", a)
    return s

def _to_utc(ts: str) -> datetime:
    # PVGIS returns ISO times; treat as local if naive, then to UTC
    dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=EU_CPH)
    return dt.astimezone(timezone.utc)

def fetch_pvgis_series(
    lat: float,
    lon: float,
    peak_kwp: float,
    tilt_deg: float,
    azimuth_deg: float,
    losses_pct: float,
    start_year: int,
    end_year: int,
    use_horizon: int = 1,
) -> Dict[str, object]:
    """
    Calls PVGIS 'seriescalc' to get HOURLY PV output for the given years and system.
    Returns dict with metadata + points: [{"ts": utc_dt, "pv_kw": float}, ...]
    """
    params = {
        "lat": f"{lat:.6f}",
        "lon": f"{lon:.6f}",
        "peakpower": f"{peak_kwp:.3f}",      # kWp
        "loss": f"{losses_pct:.1f}",         # %, total system losses
        "angle": f"{tilt_deg:.1f}",          # tilt
        "aspect": f"{azimuth_deg:.1f}",      # 0 = south, -90 = east, 90 = west
        "startyear": str(start_year),
        "endyear": str(end_year),
        "usehorizon": str(use_horizon),
        "outputformat": "json",
        "pvcalculation": "1",
        "pvtechchoice": "crystSi",
        "mountingplace": "free",
        "raddatabase": "PVGIS-SARAH3",
    }
    sess = _session()
    r = sess.get(PVGIS_BASE, params=params, timeout=60)
    if r.status_code >= 400:
        raise RuntimeError(f"PVGIS {r.status_code} {r.url}\n{r.text[:600]}")

    js = r.json()
    hourly = js.get("outputs", {}).get("hourly", [])
    points: List[Dict[str, object]] = []
    for row in hourly:
        ts = _to_utc(row["time"])
        if "P" not in row:
            raise RuntimeError("PVGIS response missing 'P' — set pvcalculation=1 and provide peakpower & loss")
        p_w = float(row["P"])
        points.append({"ts": ts, "pv_kw": p_w / 1000.0})
    points.sort(key=lambda x: x["ts"])
    return {
        "ok": True,
        "params": params,
        "meta": js.get("meta", {}),
        "inputs": js.get("inputs", {}),
        "points": points,
    }

def historic_pv_kw(
        lat: float,
        lon: float,
        peak_kwp: float,
        tilt_deg: float,
        azimuth_deg: float,
        losses_pct: float,
        start_year: int,
        end_year: int,
) -> List[Dict[str, object]]:
    """Convenience wrapper: just the list of {ts, pv_kw}."""
    return fetch_pvgis_series(
        lat, lon, peak_kwp, tilt_deg, azimuth_deg, losses_pct, start_year, end_year
    )["points"]
