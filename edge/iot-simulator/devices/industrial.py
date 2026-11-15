"""
Industrial sensors: vibration, pressure, power consumption.
These go to SFC1 (security + compression chain).
Real-world analog: factory floor, industrial equipment monitoring.
"""
import random
from devices.base import BaseSensor


SENSOR_SPECS = [
    {
        "name": "Industrial Vibration Sensor",
        "type": "vibration",
        "unit": "mm/s",
        "base": 2.0,
        "std": 0.8,
        "min": 0.0,
        "max": 100.0,
    },
    {
        "name": "Industrial Pressure Sensor",
        "type": "pressure",
        "unit": "bar",
        "base": 6.0,
        "std": 0.5,
        "min": 0.0,
        "max": 25.0,
    },
    {
        "name": "Industrial Power Sensor",
        "type": "power_consumption",
        "unit": "kw",
        "base": 18.0,
        "std": 4.0,
        "min": 0.0,
        "max": 500.0,
    },
]


class IndustrialSensor(BaseSensor):

    def __init__(self, gateway_id: str):
        super().__init__(gateway_id)
        self._spec = random.choice(SENSOR_SPECS)

    @property
    def sensor_type_name(self) -> str:
        return "ind"

    @property
    def kafka_topic_key(self) -> str:
        return "sfc1_ingress"

    def _pick_reading(self) -> dict:
        value = self._spec["base"] + random.gauss(0, self._spec["std"])
        value = max(self._spec["min"], min(self._spec["max"], value))
        return {
            "name": self._spec["name"],
            "type": self._spec["type"],
            "unit": self._spec["unit"],
            "value": round(value, 3),
        }
