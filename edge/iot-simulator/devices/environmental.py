"""
Environmental sensors: temperature, humidity, CO2.
These go to SFC1 (security + compression chain).
Real-world analog: smart building, smart home sensors.
"""
import random
from devices.base import BaseSensor


# Realistic sensor specs based on common IoT hardware
SENSOR_SPECS = [
    {
        "name": "Environmental Temperature Sensor",
        "type": "temperature",
        "unit": "celsius",
        "base": 21.0,
        "std": 3.0,
        "min": -20.0,
        "max": 60.0,
    },
    {
        "name": "Environmental Humidity Sensor",
        "type": "humidity",
        "unit": "percent",
        "base": 55.0,
        "std": 8.0,
        "min": 0.0,
        "max": 100.0,
    },
    {
        "name": "Environmental CO2 Sensor",
        "type": "co2",
        "unit": "ppm",
        "base": 420.0,
        "std": 60.0,
        "min": 300.0,
        "max": 5000.0,
    },
]


class EnvironmentalSensor(BaseSensor):

    def __init__(self, gateway_id: str):
        super().__init__(gateway_id)
        # Each sensor instance is one type (not random per reading)
        self._spec = random.choice(SENSOR_SPECS)

    @property
    def sensor_type_name(self) -> str:
        return "env"

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
            "value": round(value, 2),
        }
