from __future__ import annotations
from fastapi import FastAPI
from .services.weather import historic_power, EU_CPH
from .models import PowerPoint, GreenWindow
from datetime import timedelta
from .services.wind_live import EU_CPH, current_wind_power, historic_wind_power
from typing import Optional
from fastapi import Query
from datetime import datetime, timezone
from .services.weather import load_forecast, current_power

from dotenv import load_dotenv
from .services.solar_pvgis import historic_pv_kw
load_dotenv()

app = FastAPI(title="P7 Microgrid API", version="0.1.0")

# HELPERS -----------------------------------------------------------------------
from typing import Iterable
from bisect import bisect_left

def _parse_iso_local_to_utc(s: str | None, default: datetime | None) -> datetime | None:
    """
    Parse ISO8601; if naive, assume Europe/Copenhagen, then convert to UTC.
    """
    if s is None:
        return default
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=EU_CPH)
    return dt.astimezone(timezone.utc)

def _nearest_value(ts_list, value_list, ts):
    """Return value from value_list at timestamp closest to ts."""
    if not ts_list:
        return 0.0
    i = bisect_left(ts_list, ts)
    if i <= 0:
        return float(value_list[0])
    if i >= len(ts_list):
        return float(value_list[-1])
    before_t, after_t = ts_list[i-1], ts_list[i]
    before_v, after_v = value_list[i-1], value_list[i]
    return float(before_v if (ts - before_t) <= (after_t - ts) else after_v)

def _summaries(points: list[dict]) -> dict:
    """Daily and total kWh summaries from a list of {'ts','wind_kw','solar_kw','total_kw'} (hourly)."""
    if not points:
        return {"energy_total_kwh": 0.0, "energy_wind_kwh": 0.0, "energy_solar_kwh": 0.0, "days": []}
    # hourly averages → kWh per row ~ kw * 1h
    e_wind = sum(float(p["wind_kw"]) for p in points)
    e_solar = sum(float(p["solar_kw"]) for p in points)
    # daily buckets
    days: dict[str, dict] = {}
    for p in points:
        day = p["ts"].date().isoformat()
        d = days.setdefault(day, {"kwh_total": 0.0, "kwh_wind": 0.0, "kwh_solar": 0.0})
        d["kwh_wind"] += float(p["wind_kw"])
        d["kwh_solar"] += float(p["solar_kw"])
        d["kwh_total"] += float(p["wind_kw"]) + float(p["solar_kw"])
    return {
        "energy_total_kwh": e_wind + e_solar,
        "energy_wind_kwh": e_wind,
        "energy_solar_kwh": e_solar,
        "days": [{"day": k, **v} for k, v in sorted(days.items())]
    }

def _interp_series(points: list[dict], key: str, step_minutes: int) -> list[dict]:
    """
    Linear interpolate a series of {'ts', key} to fixed spacing 'step_minutes'.
    """
    if not points:
        return []
    out: list[dict] = []
    step = timedelta(minutes=step_minutes)
    for i in range(len(points) - 1):
        t0, v0 = points[i]["ts"], float(points[i][key])
        t1, v1 = points[i+1]["ts"], float(points[i+1][key])
        if t1 <= t0:
            continue
        # emit t0
        if i == 0:
            out.append({"ts": t0, key: v0})
        # fill between
        t = t0 + step
        while t < t1 - timedelta(seconds=1e-6):
            a = (t - t0).total_seconds() / (t1 - t0).total_seconds()
            v = v0 + a * (v1 - v0)
            out.append({"ts": t, key: v})
            t += step
        # next loop will add t1 as its first point (or last emit below)
    # ensure last point
    out.append({"ts": points[-1]["ts"], key: float(points[-1][key])})
    return out

def _sum_energy_kwh(series: list[float], step_minutes: int) -> float:
    """Sum discrete series (kW) at fixed step to kWh."""
    return sum(series) * (step_minutes / 60.0)

def _find_windows(
    ts: list[datetime],
    p_kw: list[float],
    step_minutes: int,
    need_kwh: float,
    min_power_kw: float | None = None,   # if set, require every step ≥ this
    max_windows: int = 10,
) -> list[dict]:
    """
    Sliding-window search for windows that meet energy (and optional power floor).
    Returns [{'start','end','kwh','avg_kw','min_kw','steps'}].
    """
    if not ts:
        return []
    step_h = step_minutes / 60.0
    # prefix sums for O(1) energy in any window
    pref = [0.0]
    for v in p_kw:
        pref.append(pref[-1] + v * step_h)

    out = []
    n = len(ts)
    for i in range(n):
        # binary search for j where energy(i,j) >= need_kwh
        lo, hi, j = i+1, n, None
        while lo <= hi:
            mid = (lo + hi) // 2
            e = pref[mid] - pref[i]
            if e + 1e-12 >= need_kwh:
                j = mid; hi = mid - 1
            else:
                lo = mid + 1
        if j is None:
            continue
        # power floor check
        if min_power_kw is not None and min(p_kw[i:j]) + 1e-12 < min_power_kw:
            continue
        e = pref[j] - pref[i]
        dur_steps = j - i
        out.append({
            "start": ts[i], "end": ts[j-1] + timedelta(minutes=step_minutes),
            "kwh": e, "avg_kw": e / (dur_steps * step_h),
            "min_kw": min(p_kw[i:j]), "steps": dur_steps,
        })
        if len(out) >= max_windows:
            break
    return out



@app.get("/forecast/power", response_model=list[PowerPoint])
def forecast_power():
    rows = load_forecast()
    return [{"ts": r["ts"], "wind_ms": r["wind_ms"], "wind_kw": r["wind_kw"]} for r in rows]

@app.get("/power/current")
def power_current(load_kw: float = Query(0.0, ge=0.0)):
    """
    Returns current available wind kW, plus net against an optional requested load.
    """
    cur = current_power(now_utc=datetime.now(timezone.utc))
    if not cur.get("available"):
        return {"available": False, "reason": cur.get("reason", "unknown")}

    wind_kw = float(cur["wind_kw"])
    net_kw = wind_kw - load_kw
    return {
        "available": True,
        "now_utc": cur["now_utc"],
        "wind_kw": wind_kw,
        "wind_ms": float(cur["wind_ms"]),
        "requested_load_kw": float(load_kw),
        "net_kw": net_kw,
        "meets_load": net_kw >= 0,
        "source_window": cur["source_window"],
    }

@app.get("/power/historic")
def power_historic(
    load_kw: float = Query(0.0, ge=0.0),
    start: Optional[str] = Query(None, description="ISO time; if naive, treated as Europe/Copenhagen and converted to UTC"),
    end: Optional[str] = Query(None, description="ISO time; if naive, treated as Europe/Copenhagen and converted to UTC"),
    hours: int = Query(24, ge=1, le=168, description="Used only if start/end not provided; last N hours"),
):
    """
    Returns historical wind power points and net kW over a time window.

    Priority:
      - If start and end are provided, use that range.
      - Else use 'hours' ending at now (default 24h).
    """
    now = datetime.now(timezone.utc)

    def parse_dt(s: Optional[str], default: Optional[datetime]) -> Optional[datetime]:
        if s is None:
            return default
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=EU_CPH)
        return dt.astimezone(timezone.utc)

    if start or end:
        start_dt = parse_dt(start, None)
        end_dt = parse_dt(end, now if start_dt is not None else None)
        if start_dt is None or end_dt is None:
            # fallback if only one is given
            end_dt = end_dt or now
            start_dt = start_dt or (end_dt - timedelta(hours=hours))
    else:
        end_dt = now
        start_dt = now - timedelta(hours=hours)

    return historic_power(start_dt, end_dt, load_kw=load_kw, now_utc=now)


# --- LIVE WIND (DMI) ----------------------------------------------------------
@app.get("/wind/current_live")
def wind_current_live(
    lat: float = Query(57.0488),
    lon: float = Query(9.9217),
    station_id: str | None = Query(None, description="Optional DMI stationId; omit to auto-pick nearest"),
):
    return current_wind_power(lat, lon, station_id)

@app.get("/wind/historic_live")
def wind_historic_live(
    start: str | None = Query(None, description="ISO8601; naive assumed Europe/Copenhagen"),
    end: str | None = Query(None, description="ISO8601; naive assumed Europe/Copenhagen"),
    hours: int = Query(24, ge=1, le=168),
    lat: float = Query(57.0488),
    lon: float = Query(9.9217),
    station_id: str | None = Query(None),
):
    def parse_dt(s: str | None, default: datetime | None) -> datetime | None:
        if s is None:
            return default
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=EU_CPH)
        return dt.astimezone(timezone.utc)

    now = datetime.now(timezone.utc)
    if start or end:
        start_dt = parse_dt(start, None)
        end_dt = parse_dt(end, now if start_dt is not None else None)
        if start_dt is None:
            start_dt = (end_dt or now) - timedelta(hours=hours)
        if end_dt is None:
            end_dt = now
    else:
        end_dt = now
        start_dt = now - timedelta(hours=hours)

    points = historic_wind_power(lat, lon, station_id, start_dt, end_dt)
    return {"ok": True, "start_utc": start_dt.isoformat(), "end_utc": end_dt.isoformat(), "points": points}

# --- HISTORIC SOLAR (PVGIS) ---------------------------------------------------
@app.get("/solar/historic")
def solar_historic(
    lat: float = Query(57.0488),
    lon: float = Query(9.9217),
    peak_kwp: float = Query(2.0, ge=0.1, description="Simulated DC size (kWp)"),
    tilt_deg: float = Query(35.0),
    azimuth_deg: float = Query(0.0, description="0=south, -90=east, 90=west"),
    losses_pct: float = Query(14.0, ge=0.0, le=40.0),
    start_year: int = Query(2019),
    end_year: int = Query(2025),
):
    points = historic_pv_kw(lat, lon, peak_kwp, tilt_deg, azimuth_deg, losses_pct, start_year, end_year)
    return {"ok": True, "years": [start_year, end_year], "points": points}

# --- COMBINED (LIVE WIND + PVGIS SOLAR) --------------------------------------
@app.get("/power/combined_live")
def power_combined_live(
    load_kw: float = Query(0.0, ge=0.0),
    hours: int = Query(24, ge=1, le=48),
    lat: float = Query(57.0488),
    lon: float = Query(9.9217),
    station_id: str | None = Query(None),
    peak_kwp: float = Query(2.0, ge=0.1),
    tilt_deg: float = Query(35.0),
    azimuth_deg: float = Query(0.0),
    losses_pct: float = Query(14.0, ge=0.0, le=40.0),
):
    now = datetime.now(timezone.utc)
    start_dt = now - timedelta(hours=hours)
    end_dt = now
    # wind
    wind = historic_wind_power(lat, lon, station_id, start_dt, end_dt)
    # solar over covering years
    solar = historic_pv_kw(lat, lon, peak_kwp, tilt_deg, azimuth_deg, losses_pct, start_dt.year, end_dt.year)

    s_ts = [r["ts"] for r in solar]
    s_kw = [r["pv_kw"] for r in solar]

    points = []
    for w in wind:
        skw = _nearest_value(s_ts, s_kw, w["ts"])
        tot = float(w["wind_kw"]) + skw
        points.append({
            "ts": w["ts"],
            "wind_kw": float(w["wind_kw"]),
            "solar_kw": skw,
            "total_kw": tot,
            "net_kw": tot - load_kw,
            "meets_load": tot >= load_kw,
        })
    return {
        "ok": True,
        "start_utc": start_dt.isoformat(),
        "end_utc": end_dt.isoformat(),
        "load_kw": float(load_kw),
        "summary": _summaries(points),
        "points": points,
    }



@app.get("/power/combined_historic")
def power_combined_historic(
    load_kw: float = Query(0.0, ge=0.0, description="Base load to compare against (kW)"),
    start: str | None = Query(None, description="ISO8601, e.g. '2025-03-01T00:00:00'. Naive → Europe/Copenhagen."),
    end: str | None = Query(None, description="ISO8601, e.g. '2025-03-02T00:00:00'. Naive → Europe/Copenhagen."),
    hours: int = Query(24, ge=1, le=48, description="Used only if start/end not provided."),
    # location
    lat: float = Query(57.0488),
    lon: float = Query(9.9217),
    station_id: str | None = Query(None, description="Optional DMI stationId; omit to auto-pick nearest"),
    # PV parameters
    peak_kwp: float = Query(2.0, ge=0.1, description="Simulated DC size (kWp)"),
    tilt_deg: float = Query(35.0, description="PV tilt (degrees from horizontal)"),
    azimuth_deg: float = Query(0.0, description="0=south, -90=east, 90=west"),
    losses_pct: float = Query(14.0, ge=0.0, le=40.0, description="Total system losses (%)"),
):
    now = datetime.now(timezone.utc)
    if start or end:
        start_dt = _parse_iso_local_to_utc(start, None)
        end_dt = _parse_iso_local_to_utc(end, now if start_dt is not None else None)
        if start_dt is None:
            start_dt = (end_dt or now) - timedelta(hours=hours)
        if end_dt is None:
            end_dt = now
    else:
        end_dt = now
        start_dt = now - timedelta(hours=hours)

    # --- wind (DMI) in [start_dt, end_dt]
    wind = historic_wind_power(lat, lon, station_id, start_dt, end_dt)  # [{ts, wind_ms, wind_kw}]
    # --- solar (PVGIS) for the covering years
    sy, ey = start_dt.year, end_dt.year
    solar = historic_pv_kw(lat, lon, peak_kwp, tilt_deg, azimuth_deg, losses_pct, sy, ey)  # [{ts, pv_kw}]

    # Build nearest-join: take wind timestamps as the backbone (hourly), attach nearest solar hour
    s_ts = [r["ts"] for r in solar]
    s_kw = [r["pv_kw"] for r in solar]

    points = []
    for w in wind:
        skw = _nearest_value(s_ts, s_kw, w["ts"])
        tot = float(w["wind_kw"]) + skw
        points.append({
            "ts": w["ts"],
            "wind_kw": float(w["wind_kw"]),
            "solar_kw": skw,
            "total_kw": tot,
            "net_kw": tot - load_kw,
            "meets_load": tot >= load_kw,
        })

    return {
        "ok": True,
        "start_utc": start_dt.isoformat(),
        "end_utc": end_dt.isoformat(),
        "load_kw": float(load_kw),
        "params": {
            "lat": lat, "lon": lon, "station_id": station_id,
            "peak_kwp": peak_kwp, "tilt_deg": tilt_deg, "azimuth_deg": azimuth_deg, "losses_pct": losses_pct
        },
        "summary": _summaries(points),
        "points": points,  # hourly-ish points; energy per point ≈ kWh for that hour
    }

@app.get("/schedule/solar_historic")
def schedule_solar_historic(
    power_w: float = Query(..., gt=0, description="Extra power demand during the job (W)"),
    duration_min: int = Query(..., gt=0, description="Job duration (minutes)"),
    require_power_floor: bool = Query(False, description="Require PV power ≥ demand at every step"),
    step_minutes: int = Query(5, ge=1, le=60, description="Scheduling resolution"),
    # time window
    start: str | None = Query(None, description="ISO8601; naive → Europe/Copenhagen"),
    end: str | None = Query(None),
    hours: int = Query(24, ge=1, le=168),
    # PV params
    lat: float = Query(57.0488),
    lon: float = Query(9.9217),
    peak_kwp: float = Query(2.0, ge=0.1),
    tilt_deg: float = Query(35.0),
    azimuth_deg: float = Query(0.0),
    losses_pct: float = Query(14.0, ge=0.0, le=40.0),
):
    need_kwh = (power_w / 1000.0) * (duration_min / 60.0)
    now = datetime.now(timezone.utc)
    if start or end:
        start_dt = _parse_iso_local_to_utc(start, None)
        end_dt = _parse_iso_local_to_utc(end, now if start_dt is not None else None)
        if start_dt is None:
            start_dt = (end_dt or now) - timedelta(hours=hours)
        if end_dt is None:
            end_dt = now
    else:
        end_dt = now; start_dt = now - timedelta(hours=hours)

    # PVGIS hourly
    solar = historic_pv_kw(lat, lon, peak_kwp, tilt_deg, azimuth_deg, losses_pct, start_dt.year, end_dt.year)
    # clip to window
    solar = [r for r in solar if start_dt <= r["ts"] <= end_dt]
    if len(solar) < 2:
        return {"ok": True, "windows": [], "reason": "insufficient data"}

    # resample & search
    s = _interp_series(solar, "pv_kw", step_minutes)
    ts = [r["ts"] for r in s]
    pv = [r["pv_kw"] for r in s]
    windows = _find_windows(ts, pv, step_minutes, need_kwh,
                            min_power_kw=(power_w/1000.0) if require_power_floor else None)
    return {
        "ok": True,
        "job": {"power_w": power_w, "duration_min": duration_min, "need_kwh": need_kwh},
        "params": {"lat": lat, "lon": lon, "peak_kwp": peak_kwp, "tilt": tilt_deg, "azimuth": azimuth_deg, "losses_pct": losses_pct},
        "windows": windows[:10],
    }

@app.get("/schedule/wind_historic")
def schedule_wind_historic(
    power_w: float = Query(..., gt=0),
    duration_min: int = Query(..., gt=0),
    require_power_floor: bool = Query(False),
    step_minutes: int = Query(5, ge=1, le=60),
    start: str | None = Query(None),
    end: str | None = Query(None),
    hours: int = Query(24, ge=1, le=168),
    lat: float = Query(57.0488),
    lon: float = Query(9.9217),
    station_id: str | None = Query(None),
):
    need_kwh = (power_w / 1000.0) * (duration_min / 60.0)
    now = datetime.now(timezone.utc)
    if start or end:
        start_dt = _parse_iso_local_to_utc(start, None)
        end_dt = _parse_iso_local_to_utc(end, now if start_dt is not None else None)
        if start_dt is None:
            start_dt = (end_dt or now) - timedelta(hours=hours)
        if end_dt is None:
            end_dt = now
    else:
        end_dt = now; start_dt = now - timedelta(hours=hours)

    wind = historic_wind_power(lat, lon, station_id, start_dt, end_dt)  # hourly-ish
    if len(wind) < 2:
        return {"ok": True, "windows": [], "reason": "insufficient data"}

    w = _interp_series(wind, "wind_kw", step_minutes)
    ts = [r["ts"] for r in w]
    wk = [r["wind_kw"] for r in w]
    windows = _find_windows(ts, wk, step_minutes, need_kwh,
                            min_power_kw=(power_w/1000.0) if require_power_floor else None)
    return {
        "ok": True,
        "job": {"power_w": power_w, "duration_min": duration_min, "need_kwh": need_kwh},
        "params": {"lat": lat, "lon": lon, "station_id": station_id},
        "windows": windows[:10],
    }

@app.get("/schedule/combined_historic")
def schedule_combined_historic(
    power_w: float = Query(..., gt=0),
    duration_min: int = Query(..., gt=0),
    require_power_floor: bool = Query(False, description="Require total_kw ≥ demand at every step"),
    step_minutes: int = Query(5, ge=1, le=60),
    start: str | None = Query(None, description="ISO8601, e.g. '2025-03-01T00:00:00'. Naive → Europe/Copenhagen."),
    end: str | None = Query(None, description="ISO8601, e.g. '2025-04-01T00:00:00'. Naive → Europe/Copenhagen."),
    hours: int = Query(24, ge=1, le=168),
    # location + sources
    lat: float = Query(57.0488),
    lon: float = Query(9.9217),
    station_id: str | None = Query(None),
    peak_kwp: float = Query(2.0, ge=0.1),
    tilt_deg: float = Query(35.0),
    azimuth_deg: float = Query(0.0),
    losses_pct: float = Query(14.0, ge=0.0, le=40.0),
):
    need_kwh = (power_w / 1000.0) * (duration_min / 60.0)
    now = datetime.now(timezone.utc)
    if start or end:
        start_dt = _parse_iso_local_to_utc(start, None)
        end_dt = _parse_iso_local_to_utc(end, now if start_dt is not None else None)
        if start_dt is None:
            start_dt = (end_dt or now) - timedelta(hours=hours)
        if end_dt is None:
            end_dt = now
    else:
        end_dt = now; start_dt = now - timedelta(hours=hours)

    # get both sources (hourly-ish)
    wind = historic_wind_power(lat, lon, station_id, start_dt, end_dt)
    solar = historic_pv_kw(lat, lon, peak_kwp, tilt_deg, azimuth_deg, losses_pct, start_dt.year, end_dt.year)
    wind = [r for r in wind if start_dt <= r["ts"] <= end_dt]
    solar = [r for r in solar if start_dt <= r["ts"] <= end_dt]
    if len(wind) < 2 and len(solar) < 2:
        return {"ok": True, "windows": [], "reason": "insufficient data"}

    # resample both to same grid, then sum
    w = _interp_series(wind, "wind_kw", step_minutes) if wind else []
    s = _interp_series(solar, "pv_kw", step_minutes) if solar else []
    # build unified timeline
    grid = sorted(set([r["ts"] for r in (w + s)]))
    # nearest attach
    def _nearest(ts_list, vs, t):
        if not ts_list: return 0.0
        i = bisect_left(ts_list, t)
        if i <= 0: return float(vs[0])
        if i >= len(ts_list): return float(vs[-1])
        return float(vs[i-1] if (t - ts_list[i-1]) <= (ts_list[i] - t) else vs[i])

    w_ts, w_v = ([r["ts"] for r in w], [r["wind_kw"] for r in w]) if w else ([], [])
    s_ts, s_v = ([r["ts"] for r in s], [r["pv_kw"] for r in s]) if s else ([], [])
    tot_ts, tot_kw = [], []
    for t in grid:
        tot = _nearest(w_ts, w_v, t) + _nearest(s_ts, s_v, t)
        tot_ts.append(t); tot_kw.append(tot)

    windows = _find_windows(tot_ts, tot_kw, step_minutes, need_kwh,
                            min_power_kw=(power_w/1000.0) if require_power_floor else None)
    return {
        "ok": True,
        "job": {"power_w": power_w, "duration_min": duration_min, "need_kwh": need_kwh},
        "params": {"lat": lat, "lon": lon, "peak_kwp": peak_kwp, "tilt": tilt_deg, "azimuth": azimuth_deg, "losses_pct": losses_pct, "station_id": station_id},
        "windows": windows[:10],
    }
