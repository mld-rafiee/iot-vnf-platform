"""
Dynamic scaling scheduler.

Reads the config schedule and returns the correct
number of gateways and devices for the current time.

Based on real-world IoT traffic research:
  - Finley & Vesselkov (2019): daily traffic clusters into 3 patterns
  - Hajhussein & Ibnkahla (2024): daily volume fluctuations reflect human activity
  - Manufacturing/industrial traffic halves during weekends
"""
import logging
from datetime import datetime

log = logging.getLogger(__name__)


def get_current_scale(config: dict) -> tuple:
    """
    Returns (num_gateways, devices_per_gateway, scenario_name)
    based on current time and scaling mode.
    """
    scaling = config["scaling"]
    mode = scaling["mode"]

    if mode == "fixed":
        fixed = scaling["fixed"]
        return (
            fixed["gateways"],
            fixed["devices_per_gateway"],
            "fixed",
        )

    # Dynamic mode — check time of day and day of week
    now = datetime.now()
    current_hour = now.hour
    is_weekend = now.weekday() >= 5  # 5=Saturday, 6=Sunday

    schedule_key = "weekend" if is_weekend else "weekday"
    schedule = scaling["dynamic"][schedule_key]

    for slot in schedule:
        if current_hour in slot["hours"]:
            log.info(
                f"Schedule: '{slot['name']}' | "
                f"Hour: {current_hour:02d}:xx | "
                f"{'Weekend' if is_weekend else 'Weekday'} | "
                f"Gateways: {slot['gateways']} | "
                f"Devices/GW: {slot['devices_per_gateway']}"
            )
            return (
                slot["gateways"],
                slot["devices_per_gateway"],
                slot["name"],
            )

    # Fallback — should never happen if config covers all 24 hours
    log.warning("No schedule slot matched current hour. Using fixed fallback.")
    return (1, 1000, "fallback")
