## Quick start
# Run from root of microgrid-starter-denmark
# (optional) reuse your existing .venv
python -m venv .venv
.\.venv\Scripts\Activate.ps1

# Run main to load FAST API
uvicorn api.main:app --reload

# Deactivate venv
deactivate

# verify you're in the venv
python -c "import sys; print(sys.executable)"

# install libs your script uses
python -m pip install --upgrade pip
python -m pip install requests pandas numpy python-dotenv

# open swaggerUI
http://127.0.0.1:8000/docs

# File exists.
pip install -r requirements-api.txt

# PVGIS API
PVGIS seriescalc is the official non-interactive API 
for hourly PV output, where peakpower is system size in kWp,
angle/aspect are tilt/azimuth, and loss is % losses. 
This endpoint is documented by the JRC (PVGIS 5.3 API entry 
points and tool names).

# Windows PowerShell:
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
copy .env.example .env
