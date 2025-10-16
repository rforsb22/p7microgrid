from pathlib import Path
from datetime import timedelta

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
WEATHER_JSON_PATH = DATA_DIR / "weather_wind.json"
WEATHER_CSV_PATH  = DATA_DIR / "weather_wind.csv"

# Battery defaults – tune or expose via env if you like
BATTERY_CAPACITY_KWH = 10.0
INITIAL_SOC_KWH = 5.0         # 50% start
CHARGE_EFF = 0.95
DISCHARGE_EFF = 0.95
SIM_STEP = timedelta(minutes=10)  # runtime simulation step
