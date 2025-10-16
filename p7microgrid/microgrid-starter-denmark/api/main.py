from fastapi import FastAPI, HTTPException, Query
from datetime import datetime, timezone
from .models import BatteryStatus, RuntimeRequest, RuntimeEstimate, PowerPoint, GreenWindow
from .services.weather import load_forecast
from .services.battery import BatterySim
from .services.schedule import green_windows
from .models import SocTargetsRequest, SocTargetETA

app = FastAPI(title="P7 Microgrid API", version="0.1.0")

# in-memory (swap to a DB if you want persistence across restarts)
bat = BatterySim()

@app.get("/health")
def health():
    return {"ok": True}

@app.get("/forecast/power", response_model=list[PowerPoint])
def forecast_power():
    rows = load_forecast()
    return [{"ts": r["ts"], "wind_ms": r["wind_ms"], "wind_kw": r["wind_kw"]} for r in rows]

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


