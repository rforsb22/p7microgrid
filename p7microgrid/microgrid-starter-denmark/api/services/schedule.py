from typing import List, Dict
from datetime import datetime, timedelta
from ..models import GreenWindow

def green_windows(forecast: List[Dict], load_kw: float, min_block_minutes=30) -> List[GreenWindow]:
    """
    Return windows where wind >= load for at least min_block_minutes.
    """
    if not forecast:
        return []
    out = []
    start = None
    acc_margin = 0.0
    count = 0
    for i, r in enumerate(forecast):
        margin = r["wind_kw"] - load_kw
        if margin >= 0:
            if start is None:
                start = r["ts"]
                acc_margin = 0.0
                count = 0
            acc_margin += margin
            count += 1
        else:
            if start:
                end = forecast[i]["ts"]
                minutes = int((end - start).total_seconds() // 60)
                if minutes >= min_block_minutes:
                    out.append(GreenWindow(start=start, end=end, avg_margin_kw=acc_margin/max(count,1)))
                start = None
    # tail
    if start:
        end = forecast[-1]["ts"]
        minutes = int((end - start).total_seconds() // 60)
        if minutes >= min_block_minutes:
            out.append(GreenWindow(start=start, end=end, avg_margin_kw=acc_margin/max(count,1)))
    return out
