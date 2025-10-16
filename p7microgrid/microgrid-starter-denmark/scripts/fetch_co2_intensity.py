# scripts/fetch_co2_intensity.py
import os, json, math, pandas as pd, numpy as np, requests
from pathlib import Path
from dotenv import load_dotenv

DATA = Path("data")
CO2_OUT = DATA / "co2_intensity.csv"  # time, co2_g_per_kwh

def _from_minstroem(url: str) -> pd.DataFrame:
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    js = r.json()

    # Try to be permissive about structure: accept list[ {time: ..., co2: ...} ] or a dict with "records"
    if isinstance(js, dict) and "records" in js:
        recs = js["records"]
    elif isinstance(js, list):
        recs = js
    else:
        raise ValueError(f"Unexpected JSON structure from MinStrøm: top-level keys={list(js.keys()) if isinstance(js, dict) else type(js)}")

    df = pd.DataFrame(recs)
    # try common column names
    time_col = next((c for c in df.columns if c.lower() in ("time","datetime","timestamp","minutes15dk","hourdk")), None)
    co2_col  = next((c for c in df.columns if "co2" in c.lower() and ("g/kwh" in c.lower() or "g_per_kwh" in c.lower() or "intensity" in c.lower())), None)

    if not time_col:
        raise ValueError(f"Could not find a time column in MinStrøm response. Columns: {list(df.columns)}")
    if not co2_col:
        # try a couple more generic guesses
        co2_col = next((c for c in df.columns if "co2" in c.lower()), None)
    if not co2_col:
        raise ValueError(f"Could not find a CO2 column in MinStrøm response. Columns: {list(df.columns)}")

    df[time_col] = pd.to_datetime(df[time_col])
    df = df.rename(columns={time_col: "time", co2_col: "co2_g_per_kwh"})[["time", "co2_g_per_kwh"]]
    df = df.sort_values("time").reset_index(drop=True)
    return df

def _from_wind_proxy() -> pd.DataFrame:
    """Fallback: use your weather_wind.csv as a crude proxy for greenness (more wind -> lower CO2)."""
    wfile = DATA / "weather_wind.csv"
    if not wfile.exists():
        raise SystemExit("No MINSTROEM_CO2_URL and no data/weather_wind.csv. Run scripts/fetch_weather.py or set MINSTROEM_CO2_URL in .env")
    w = pd.read_csv(wfile, parse_dates=["time"]).sort_values("time")
    # normalize wind to [0,1]
    wind_norm = (w["wind_kw"] - w["wind_kw"].min()) / (w["wind_kw"].max() - w["wind_kw"].min() + 1e-9)
    # map to a plausible CO2 range [50..400] g/kWh (purely illustrative)
    co2 = 400.0 - wind_norm * (400.0 - 50.0)
    return pd.DataFrame({"time": w["time"], "co2_g_per_kwh": co2})

if __name__ == "__main__":
    load_dotenv()
    url = (os.getenv("MINSTROEM_CO2_URL") or "").strip()

    if url:
        try:
            df = _from_minstroem(url)
            df.to_csv(CO2_OUT, index=False)
            print(f"Wrote {CO2_OUT} from MinStrøm ({len(df)} rows)")
        except Exception as e:
            print(f"[MinStrøm] {e}\nFalling back to wind-based proxy…")
            df = _from_wind_proxy()
            df.to_csv(CO2_OUT, index=False)
            print(f"Wrote {CO2_OUT} (wind-based proxy) ({len(df)} rows)")
    else:
        df = _from_wind_proxy()
        df.to_csv(CO2_OUT, index=False)
        print(f"Wrote {CO2_OUT} (wind-based proxy) ({len(df)} rows)")
