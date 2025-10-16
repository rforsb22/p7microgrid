import os
import pandas as pd
import requests
from dotenv import load_dotenv

OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"

def _call_open_meteo(params):
    r = requests.get(OPEN_METEO_URL, params=params, timeout=30)
    r.raise_for_status()
    return r.json()

def fetch_wind(lat: float, lon: float, hours: int = 168) -> pd.DataFrame:
    base_params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": "wind_speed_10m",
        "forecast_days": 7,
        "past_days": 2,           # gives some recent history too
        "windspeed_unit": "ms",   # <-- FIX (no slash)
        "timezone": "Europe/Copenhagen",
    }

    try:
        # prefer DMI model if available
        js = _call_open_meteo({**base_params, "models": "dmi_seamless"})
    except requests.HTTPError as e:
        # fallback to auto model selection
        js = _call_open_meteo(base_params)

    times = pd.to_datetime(js["hourly"]["time"])
    wind = pd.Series(js["hourly"]["wind_speed_10m"], index=times, name="wind_speed_10m")
    df = wind.to_frame().reset_index().rename(columns={"index": "time"})
    return df.iloc[:hours]

def simple_wind_to_power(wind_speed_ms: pd.Series, rated_kw: float) -> pd.Series:
    def p(v):
        if v < 3.0 or v >= 25.0:
            return 0.0
        if v >= 12.0:
            return rated_kw
        return rated_kw * ((v - 3.0) / (12.0 - 3.0)) ** 3
    return wind_speed_ms.apply(p)

if __name__ == "__main__":
    load_dotenv()
    lat = float(os.getenv("LAT", "57.0488"))
    lon = float(os.getenv("LON", "9.9217"))
    df = fetch_wind(lat, lon, hours=168)
    df["wind_kw"] = simple_wind_to_power(df["wind_speed_10m"], rated_kw=10.0)
    out = "data/weather_wind.csv"
    df.to_csv(out, index=False)
    print(f"Wrote {out} ({len(df)} rows)")
