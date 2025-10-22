## Quick start
# Run from root of microgrid-starter-denmark
# (optional) reuse your existing .venv
python -m venv .venv
.\.venv\Scripts\Activate.ps1

# Run to get lastest weather data from API -> Check data/weather_wind.json
python ./scripts/fetch_weather.py

# Run main to laod FAST API
uvicorn api.main:app --reload




# Deactivate venv
deactivate

# verify you're in the venv
python -c "import sys; print(sys.executable)"

# install libs your script uses
python -m pip install --upgrade pip
python -m pip install requests pandas numpy python-dotenv

# open battery status site
http://127.0.0.1:8000/battery/status

# open api/docs
http://127.0.0.1:8000/docs

# File exists.
pip install -r requirements-api.txt


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