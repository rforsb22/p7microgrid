from __future__ import annotations
from typing import List, Dict
from pathlib import Path
import json, csv
import pandas as pd
from datetime import datetime
import zoneinfo
from ..config import WEATHER_JSON_PATH, WEATHER_CSV_PATH

_CPH = zoneinfo.ZoneInfo("Europe/Copenhagen")

def _parse_local(s: str) -> datetime:
    # Your JSON times are local DK with no tz → attach Copenhagen tz
    dt = datetime.fromisoformat(s)
    return dt.replace(tzinfo=_CPH).astimezone(zoneinfo.ZoneInfo("UTC"))

def load_forecast() -> List[Dict]:
    """
    Load rows like:
      {"ts": utc_datetime, "wind_ms": float, "wind_kw": float}
    from your weather_wind.json (preferred) or CSV fallback.
    """
    if WEATHER_JSON_PATH.exists():
        with open(WEATHER_JSON_PATH, "r", encoding="utf-8") as f:
            raw = json.load(f)
        rows = []
        for r in raw:
            rows.append({
                "ts": _parse_local(r["time"]),
                "wind_ms": float(r["wind_ms"]),
                "wind_kw": float(r.get("wind_kw", 0.0)),
            })
        return sorted(rows, key=lambda x: x["ts"])

    if WEATHER_CSV_PATH.exists():
        df = pd.read_csv(WEATHER_CSV_PATH)
        df["ts"] = pd.to_datetime(df["time"]).dt.tz_localize(_CPH).dt.tz_convert("UTC")
        df["wind_ms"] = df["wind_ms"].astype(float)
        if "wind_kw" not in df:
            df["wind_kw"] = 0.0
        return df.sort_values("ts")[["ts","wind_ms","wind_kw"]].to_dict("records")

    raise FileNotFoundError("No weather_wind.json or weather_wind.csv found in /data")
