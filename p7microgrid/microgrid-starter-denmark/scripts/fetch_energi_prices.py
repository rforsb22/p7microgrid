import os, json, requests, pandas as pd
from urllib.parse import urlencode, quote
from dotenv import load_dotenv

EDS_BASE = "https://api.energidataservice.dk/dataset"
CANDIDATE_DATASETS = ["DayAheadPrices", "Elspotprices"]

REL_START = "now-P2D"     # Danish local time window: 2 days back…
REL_END   = "now%2BP2D"   # …and 2 days forward (note: + must be %2B)

def build_url(dataset, area, limit=None):
    params = {
        "start": REL_START,
        "end": REL_END,
        "filter": json.dumps({"PriceArea": [area]}),
    }
    if limit is not None:
        params["limit"] = limit
    return f"{EDS_BASE}/{dataset}?{urlencode(params, quote_via=quote)}"

def pick_time_col(df):
    for c in ["Minutes15DK","Minutes5DK","HourDK"]:
        if c in df.columns:
            return c
    # fallback: any DK-ending datetime-like column
    for c in df.columns:
        if c.lower().endswith("dk"):
            return c
    raise ValueError("Couldn't find a DK time column in price data.")

def compute_dkk_per_kwh(df):
    # Prefer native DKK/MWh if present
    dkk_cols = [c for c in df.columns if "dk" in c.lower() and "price" in c.lower()]
    dkk_cols = [c for c in dkk_cols if "dkk" in c.lower()]  # e.g., PriceDKK
    if dkk_cols:
        dkk_mwh = pd.to_numeric(df[dkk_cols[0]], errors="coerce")
        return dkk_mwh / 1000.0  # -> DKK/kWh

    # Else convert from EUR/MWh
    eur_cols = [c for c in df.columns if "eur" in c.lower() and "price" in c.lower()]
    if eur_cols:
        eur_mwh = pd.to_numeric(df[eur_cols[0]], errors="coerce")
        rate = float(os.getenv("DKK_PER_EUR", "7.45"))
        return (eur_mwh * rate) / 1000.0  # -> DKK/kWh

    # Legacy Elspotprices name
    if "SpotPriceEUR" in df.columns:
        rate = float(os.getenv("DKK_PER_EUR", "7.45"))
        return (pd.to_numeric(df["SpotPriceEUR"], errors="coerce") * rate) / 1000.0

    raise ValueError(f"Couldn't find a DKK or EUR price column to compute DKK/kWh. Columns: {list(df.columns)}")

def fetch_prices(area):
    last_err = None
    for ds in CANDIDATE_DATASETS:
        url = build_url(ds, area, limit=5000)
        try:
            r = requests.get(url, timeout=30)
            r.raise_for_status()
            recs = r.json().get("records", [])
            if not recs:
                continue
            df = pd.DataFrame(recs)
            tcol = pick_time_col(df)
            df[tcol] = pd.to_datetime(df[tcol])
            df["price_dkk_per_kwh"] = compute_dkk_per_kwh(df)
            out = df[[tcol, "PriceArea", "price_dkk_per_kwh"]].rename(columns={tcol: "time_dk"})
            return out.sort_values("time_dk").reset_index(drop=True), ds
        except Exception as e:
            last_err = e
            continue
    raise RuntimeError(f"All price dataset attempts failed. Last error: {last_err}")

if __name__ == "__main__":
    load_dotenv()
    area = (os.getenv("PRICE_AREA") or "DK1").strip().upper()
    df, used = fetch_prices(area)
    out = "data/elspot_prices.csv"
    df.to_csv(out, index=False)
    print(f"Wrote {out} ({len(df)} rows) using dataset: {used} (DKK/kWh)")
