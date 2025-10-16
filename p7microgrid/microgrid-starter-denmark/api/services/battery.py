from datetime import datetime, timedelta
from typing import List, Dict
from ..config import (
    BATTERY_CAPACITY_KWH, INITIAL_SOC_KWH, CHARGE_EFF, DISCHARGE_EFF, SIM_STEP
)

class BatterySim:
    def __init__(self, capacity_kwh: float = BATTERY_CAPACITY_KWH, soc_kwh: float = INITIAL_SOC_KWH):
        self.capacity = float(capacity_kwh)
        self.soc = max(0.0, min(float(soc_kwh), self.capacity))

    def status(self, as_of: datetime) -> Dict:
        pct = 100.0 * self.soc / self.capacity if self.capacity > 0 else 0.0
        return {"now_soc_kwh": self.soc, "capacity_kwh": self.capacity, "soc_pct": pct, "as_of": as_of}

    def step(self, gen_kw: float, load_kw: float, dt: timedelta):
        h = dt.total_seconds()/3600.0
        # charge then discharge
        self.soc = min(self.capacity, self.soc + max(0.0, gen_kw) * CHARGE_EFF * h)
        self.soc = max(0.0, self.soc - max(0.0, load_kw) * h / DISCHARGE_EFF)

        # ADD this method inside BatterySim
        def simulate_until_targets(
                self, forecast: List[Dict], targets_kwh: List[float], base_load_kw: float, start: datetime
        ) -> Dict[float, Dict]:
            """
            Returns a map target_kwh -> { 'eta': datetime | None, 'reachable': bool }.
            Does not mutate the caller if you run it on a clone.
            """
            remaining = sorted(set(round(t, 6) for t in targets_kwh if t > self.soc))
            reached = {t: {"eta": start, "reachable": True} for t in targets_kwh if t <= self.soc}

            i, t = 0, start
            while remaining:
                while i + 1 < len(forecast) and forecast[i + 1]["ts"] <= t:
                    i += 1
                gen_kw = forecast[i]["wind_kw"] if i < len(forecast) else 0.0
                self.step(gen_kw=gen_kw, load_kw=base_load_kw, dt=SIM_STEP)
                t += SIM_STEP

                newly_reached = [tr for tr in remaining if self.soc >= tr - 1e-9]
                for tr in newly_reached:
                    reached[tr] = {"eta": t, "reachable": True}
                remaining = [tr for tr in remaining if tr not in newly_reached]

                if i >= len(forecast) - 1 and remaining:
                    # Forecast exhausted; mark remaining as unreachable
                    for tr in remaining:
                        reached[tr] = {"eta": None, "reachable": False}
                    break

            # Fill any still-unset (e.g., empty forecast)
            for tr in targets_kwh:
                reached.setdefault(tr, {"eta": None, "reachable": self.soc >= tr - 1e-9})
            return reached

    def simulate_runtime(self, forecast: List[Dict], load_kw: float, start: datetime) -> Dict:
        i, t = 0, start
        start_soc = self.soc
        while True:
            while i + 1 < len(forecast) and forecast[i + 1]["ts"] <= t:
                i += 1
            gen_kw = forecast[i]["wind_kw"] if i < len(forecast) else 0.0
            self.step(gen_kw, load_kw, SIM_STEP)
            t += SIM_STEP
            if self.soc <= 1e-6:
                return {
                    "load_kw": load_kw, "start_soc_kwh": start_soc,
                    "runtime_minutes": int((t - start).total_seconds() // 60),
                    "end_time": t, "depleted": True
                }
            if i >= len(forecast) - 1:
                return {
                    "load_kw": load_kw, "start_soc_kwh": start_soc,
                    "runtime_minutes": int((t - start).total_seconds() // 60),
                    "end_time": t, "depleted": False
                }
