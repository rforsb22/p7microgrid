import random
from dataclasses import dataclass
from typing import Tuple, Optional
from datetime import datetime, time


@dataclass
class Task:
    id: int
    energy_kwh: float


def simulate_wind_forecast(probability_enough_wind: float = 0.7) -> bool:
    """
    For nu er der 70% chance for nok vind, afventer data fra DMI og hvordan vi
    vil opdele og udregne vindstyrke med mængde af strøm microgrid kan producere
    """
    p = max(0.0, min(1.0, probability_enough_wind))
    return random.random() < p


def is_microgrid_window(now: Optional[datetime] = None) -> bool:
    """
    Return True if current local time is between 16:00 and 23:59 inclusive.
    Times later than 23:59:59 are not allowed.
    """
    now = now or datetime.now()
    return time(16, 0) <= now.time() <= time(23, 59, 59)


def schedule_single(task: Task, enough_wind: bool, threshold_kwh: float, now: Optional[datetime] = None) -> Tuple[
    str, Optional[int]]:
    """
    Schedule one task to microgrid or simulated grid based on wind and time window.
    """
    if not enough_wind:
        return ("simulated", None)
    if not is_microgrid_window(now):
        return ("simulated", None)
    if task.energy_kwh <= threshold_kwh:
        return ("microgrid", 0)
    return ("simulated", None)


def random_time_today() -> datetime:
    """
    Generate a random time today (local) with minute resolution.
    """
    today = datetime.now()
    hour = random.randint(0, 23)
    minute = random.randint(0, 59)
    return today.replace(hour=hour, minute=minute, second=0, microsecond=0)


if __name__ == "__main__":
    # Simulate random wind condition
    enough_wind = simulate_wind_forecast(probability_enough_wind=0.7)
    print("Enough wind forecasted?:", enough_wind)
    print("Microgrid time window active?:", is_microgrid_window())

    # Define a single task
    task = Task(id=1, energy_kwh=50)
    threshold = 50.0  # placeholder til vi ved hvor meget strøm er nok

    # Schedule the task (using current time) Commented out for now for later use if we want realtime checks
    # node, slot = schedule_single(task, enough_wind, threshold)
    # print("\n--- Scheduling Result (now) ---")
    # if node == "microgrid":
    #    print(f"Task {task.id}: scheduled on microgrid")
    # else:
    #    print(f"Task {task.id}: insufficient wind or outside allowed time → simulated grid")
    #
    print("\n--- Randomized checks (time + wind) ---")
    print(f"{'Time':<6}| {'Enough Wind':<11} | {'Microgrid Window':<16} | {'Scheduled':<12}")
    print("-" * 55)

for _ in range(10):
    t_now = random_time_today()
    enough = simulate_wind_forecast(probability_enough_wind=0.7)
    microgrid_active = is_microgrid_window(t_now)
    node, _ = schedule_single(task, enough, threshold, now=t_now)

    if node == "microgrid":
        status = "Microgrid"
    else:
        status = "Simulated"

    print(f"{t_now.strftime('%H:%M')} | {str(enough):<11} | {str(microgrid_active):<16} | {status:<12}")
