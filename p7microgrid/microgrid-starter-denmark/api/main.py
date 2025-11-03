from fastapi import FastAPI, HTTPException, Query

from .models import BatteryStatus, RuntimeRequest, RuntimeEstimate, PowerPoint, GreenWindow
from .services.weather import load_forecast
from .services.battery import BatterySim
from .services.schedule import green_windows
from .models import SocTargetsRequest, SocTargetETA
from typing import Optional
from fastapi import Query
from datetime import datetime, timezone, timedelta
from .services.weather import historic_power, EU_CPH

app = FastAPI(title="P7 Microgrid API", version="0.1.0")

# in-memory (swap to a DB if you want persistence across restarts)
bat = BatterySim()

@app.get("/health")
def health():
    return {"ok": True}

if __name__ == "__main__":
    import uvicorn
    # change host/port here
    uvicorn.run(app, host="0.0.0.0", port=8000)

@app.get("/forecast/power", response_model=list[PowerPoint])
def forecast_power():
    rows = load_forecast()
    return [{"ts": r["ts"], "wind_ms": r["wind_ms"], "wind_kw": r["wind_kw"]} for r in rows]

from typing import Optional
from fastapi import Query
from datetime import datetime, timezone
from .services.weather import load_forecast, current_power

# ...

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











@app.get("/health/edr")
def health_edr():
    """
    Quick health: how many points, and time range loaded.
    """
    rows = load_forecast()
    if not rows:
        return {"ok": False, "points": 0}
    return {
        "ok": True,
        "points": len(rows),
        "start_utc": rows[0]["ts"].isoformat(),
        "end_utc": rows[-1]["ts"].isoformat(),
    }


@app.get("/battery/status", response_model=BatteryStatus)
def battery_status():
    return BatteryStatus(**bat.status(as_of=datetime.now(timezone.utc)))

@app.post("/battery/estimate-runtime", response_model=RuntimeEstimate)
def estimate_runtime(req: RuntimeRequest):
    if req.load_kw < 0:
        raise HTTPException(400, "load_kw must be >= 0")
    rows = load_forecast()
    sim = BatterySim(capacity_kwh=bat.capacity, soc_kwh=bat.soc)  # copy so we don't mutate state
    now = datetime.now(timezone.utc)
    result = sim.simulate_runtime(rows, load_kw=req.load_kw, start=now)
    return result

# ADD this endpoint (e.g., below /battery/estimate-runtime)
@app.post("/battery/when-soc", response_model=list[SocTargetETA])
def when_soc(req: SocTargetsRequest):
    if (not req.targets_pct) and (not req.targets_kwh):
        # default thresholds if none provided
        req.targets_pct = [50.0, 80.0, 100.0]

    if req.base_load_kw < 0:
        raise HTTPException(400, "base_load_kw must be >= 0")

    rows = load_forecast()
    now = datetime.now(timezone.utc)

    # Build absolute targets in kWh
    targets_kwh = []
    if req.targets_pct:
        for p in req.targets_pct:
            if p < 0 or p > 100:
                raise HTTPException(400, "targets_pct must be in [0,100]")
            targets_kwh.append(bat.capacity * (p / 100.0))
    if req.targets_kwh:
        for k in req.targets_kwh:
            if k < 0 or k > bat.capacity:
                raise HTTPException(400, f"targets_kwh values must be within [0,{bat.capacity}]")
            targets_kwh.append(k)

    # Simulate on a copy so we don't mutate live state
    sim = BatterySim(capacity_kwh=bat.capacity, soc_kwh=bat.soc)
    hits = sim.simulate_until_targets(rows, targets_kwh=targets_kwh, base_load_kw=req.base_load_kw, start=now)

    # Build response sorted by target_kwh
    out: list[SocTargetETA] = []
    for tk in sorted(set(targets_kwh)):
        pct = 100.0 * tk / bat.capacity if bat.capacity > 0 else 0.0
        h = hits.get(tk, {"eta": None, "reachable": False})
        out.append(SocTargetETA(target_pct=pct, target_kwh=tk, eta=h["eta"], reachable=h["reachable"]))
    return out


@app.post("/battery/set-soc")
def set_soc(kwh: float = Query(..., ge=0)):
    bat.soc = min(kwh, bat.capacity)
    return {"now_soc_kwh": bat.soc}



@app.get("/schedule/green", response_model=list[GreenWindow])
def schedule_green(load_kw: float = Query(..., ge=0), min_block_minutes: int = Query(30, ge=10)):
    rows = load_forecast()
    return green_windows(rows, load_kw=load_kw, min_block_minutes=min_block_minutes)


