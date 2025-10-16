## Quick start
# Run from repo root:
# (optional) reuse your existing .venv
python -m venv .venv
source .venv/bin/activate

pip install -r requirements-api.txt
uvicorn api.main:app --reload

# open site
http://127.0.0.1:8000/battery/status

# api/docs
http://127.0.0.1:8000/docs



# Microgrid Starter (Denmark)
Quick local microgrid sim using python-microgrid + Energi Data Service (prices) + DMI-based wind via Open-Meteo.
## Quick start OLD
python -m venv .venv
# Windows PowerShell:
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
copy .env.example .env
# edit .env (PRICE_AREA=DK1 or DK2; optional LAT/LON)
python scripts/fetch_energi_prices.py
python scripts/fetch_weather.py
python simulator/run_sim.py