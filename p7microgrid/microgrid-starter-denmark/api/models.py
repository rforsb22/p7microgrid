from pydantic import BaseModel
from datetime import datetime
from typing import List, Optional

class BatteryStatus(BaseModel):
    now_soc_kwh: float
    capacity_kwh: float
    soc_pct: float
    as_of: datetime

class RuntimeRequest(BaseModel):
    load_kw: float

class RuntimeEstimate(BaseModel):
    load_kw: float
    start_soc_kwh: float
    runtime_minutes: int
    end_time: datetime
    depleted: bool

class PowerPoint(BaseModel):
    ts: datetime
    wind_ms: float
    wind_kw: float

class GreenWindow(BaseModel):
    start: datetime
    end: datetime
    avg_margin_kw: float  # avg (wind_kw - load_kw) in window

class SocTargetsRequest(BaseModel):
    # Either targets_pct (0–100) or targets_kwh (absolute). If both given, both are used.
    targets_pct: Optional[list[float]] = None
    targets_kwh: Optional[list[float]] = None
    base_load_kw: float = 0.0  # constant background load while waiting

class SocTargetETA(BaseModel):
    target_pct: float
    target_kwh: float
    eta: Optional[datetime]  # null if not reached within forecast
    reachable: bool