# services/weather.py
from pathlib import Path
from datetime import datetime, timezone
from typing import Optional, Tuple, Dict, Any, List
import json

from zoneinfo import ZoneInfo

WEATHER_JSON = Path("data/weather_wind.json")
EU_CPH = ZoneInfo("Europe/Copenhagen")

def _rows_to_utc(rows: List[dict]) -> List[dict]:
    out = []
    for r in rows:
        ts = datetime.fromisoformat(r["time"])
        # Our file stores local-naive timestamps (Europe/Copenhagen, no tzinfo).
        # Treat them as local and convert to UTC for consistent math.
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=EU_CPH).astimezone(timezone.utc)
        out.append({"ts": ts, "wind_ms": float(r["wind_ms"]), "wind_kw": float(r["wind_kw"])})
    # sort by ts just in case
    out.sort(key=lambda x: x["ts"])
    return out

def load_forecast() -> List[Dict[str, Any]]:
    if not WEATHER_JSON.exists():
        return []
    with open(WEATHER_JSON, "r", encoding="utf-8") as f:
        rows = json.load(f)
    return _rows_to_utc(rows)

def _bracketing_points(
    series: List[Dict[str, Any]], ts: datetime
) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
    """
    Return the nearest points p0<=ts and p1>=ts for interpolation.
    """
    if not series:
        return None, None
    # assume sorted
    if ts <= series[0]["ts"]:
        return None, series[0]
    if ts >= series[-1]["ts"]:
        return series[-1], None
    # binary-ish scan (linear is fine for 48–72 points)
    for i in range(1, len(series)):
        if series[i]["ts"] >= ts:
            return series[i - 1], series[i]
    return series[-1], None

def _lerp(a: float, b: float, t: float) -> float:
    return a + (b - a) * t

def current_power(now_utc: Optional[datetime] = None) -> Dict[str, Any]:
    """
    Returns the current estimated wind power based on forecast,
    linearly interpolated between the two nearest forecast samples.
    """
    rows = load_forecast()
    if not rows:
        return {"available": False, "reason": "no_forecast"}

    now = now_utc or datetime.now(timezone.utc)
    p0, p1 = _bracketing_points(rows, now)

    # exact match (or before/after edges)
    if p0 and p1 and p0["ts"] == now:
        wind_kw = p0["wind_kw"]
        wind_ms = p0["wind_ms"]
        src = {"type": "exact", "prev": p0["ts"].isoformat(), "next": p1["ts"].isoformat()}
    elif p0 and p1:
        # linear interpolation in time
        total = (p1["ts"] - p0["ts"]).total_seconds()
        part = (now - p0["ts"]).total_seconds()
        t = 0.0 if total <= 0 else max(0.0, min(1.0, part / total))
        wind_kw = _lerp(p0["wind_kw"], p1["wind_kw"], t)
        wind_ms = _lerp(p0["wind_ms"], p1["wind_ms"], t)
        src = {"type": "interpolated", "prev": p0["ts"].isoformat(), "next": p1["ts"].isoformat(), "t": t}
    elif p0 and not p1:
        # after last point — hold last value (or mark unavailable, your choice)
        wind_kw = p0["wind_kw"]
        wind_ms = p0["wind_ms"]
        src = {"type": "extrapolated_last", "prev": p0["ts"].isoformat(), "next": None}
    else:
        # before first point — hold first value
        wind_kw = p1["wind_kw"]
        wind_ms = p1["wind_ms"]
        src = {"type": "extrapolated_first", "prev": None, "next": p1["ts"].isoformat()}

    return {
        "available": True,
        "now_utc": now.isoformat(),
        "wind_ms": wind_ms,
        "wind_kw": wind_kw,
        "source_window": src,
    }

# --- Historic power helper ----------------------------------------------------
from typing import Optional, Dict, Any, List
from zoneinfo import ZoneInfo
from datetime import timedelta

EU_CPH = ZoneInfo("Europe/Copenhagen")

def _parse_when(s: Optional[str], default: Optional[datetime] = None) -> Optional[datetime]:
    """Parse ISO8601; if naive, assume Europe/Copenhagen, then convert to UTC."""
    if s is None:
        return default
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=EU_CPH)
    return dt.astimezone(timezone.utc)

def _integrate_energy_kwh(points: List[Dict[str, Any]], key: str) -> float:
    """Simple trapezoidal integration over (possibly uneven) time deltas."""
    if len(points) < 2:
        return 0.0
    total = 0.0
    for i in range(1, len(points)):
        t0, t1 = points[i-1]["ts"], points[i]["ts"]
        p0, p1 = float(points[i-1][key]), float(points[i][key])
        dh = (t1 - t0).total_seconds() / 3600.0
        if dh > 0:
            total += 0.5 * (p0 + p1) * dh
    return total

def historic_power(
    start_utc: Optional[datetime] = None,
    end_utc: Optional[datetime] = None,
    load_kw: float = 0.0,
    now_utc: Optional[datetime] = None,
) -> Dict[str, Any]:
    """
    Returns historical wind kW and net kW for the window [start_utc, end_utc].
    If start/end are None, defaults to last 24 hours ending at now.
    Only includes points <= now (no future).
    """
    rows = load_forecast()  # [{"ts": UTC datetime, "wind_ms": float, "wind_kw": float}, ...]
    if not rows:
        return {"ok": False, "reason": "no_forecast", "points": []}

    now = now_utc or datetime.now(timezone.utc)
    # Default window: last 24h
    if end_utc is None:
        end_utc = now
    if start_utc is None:
        start_utc = end_utc - timedelta(hours=24)

    # Clamp to available + not in future
    start_utc = max(start_utc, rows[0]["ts"])
    end_utc = min(end_utc, rows[-1]["ts"], now)
    if start_utc >= end_utc:
        return {"ok": True, "points": [], "start_utc": start_utc.isoformat(), "end_utc": end_utc.isoformat()}

    # Filter
    series = [r for r in rows if start_utc <= r["ts"] <= end_utc]

    # Build output points with net
    pts = []
    for r in series:
        wind_kw = float(r["wind_kw"])
        pts.append({
            "ts": r["ts"],
            "wind_ms": float(r["wind_ms"]),
            "wind_kw": wind_kw,
            "net_kw": wind_kw - load_kw,
            "meets_load": wind_kw >= load_kw,
        })

    # Summary stats
    if pts:
        avg_wind_kw = sum(p["wind_kw"] for p in pts) / len(pts)
        min_wind_kw = min(p["wind_kw"] for p in pts)
        max_wind_kw = max(p["wind_kw"] for p in pts)
        energy_wind_kwh = _integrate_energy_kwh(pts, "wind_kw")
        # Net is just (wind_kw - load_kw) integrated
        # An easy way: integrate wind then subtract load * hours
        hours = (pts[-1]["ts"] - pts[0]["ts"]).total_seconds() / 3600.0
        energy_net_kwh = energy_wind_kwh - load_kw * max(0.0, hours)
    else:
        avg_wind_kw = min_wind_kw = max_wind_kw = energy_wind_kwh = energy_net_kwh = 0.0

    return {
        "ok": True,
        "start_utc": start_utc.isoformat(),
        "end_utc": end_utc.isoformat(),
        "load_kw": float(load_kw),
        "points": pts,  # ts are UTC datetimes; FastAPI will serialize to ISO
        "summary": {
            "avg_wind_kw": avg_wind_kw,
            "min_wind_kw": min_wind_kw,
            "max_wind_kw": max_wind_kw,
            "energy_wind_kwh": energy_wind_kwh,
            "energy_net_kwh": energy_net_kwh,
        },
    }

