"""
Network edge device sensors: packet loss, latency, throughput.
These go to SFC2 (NAT + routing chain).
Real-world analog: network probes, edge routers, 5G base stations.
"""
import random
from devices.base import BaseSensor


SENSOR_SPECS = [
    {
        "name": "Network Packet Loss Sensor",
        "type": "packet_loss",
        "unit": "percent",
        "base": 0.5,
        "std": 0.3,
        "min": 0.0,
        "max": 100.0,
    },
    {
        "name": "Network Latency Sensor",
        "type": "latency",
        "unit": "ms",
        "base": 25.0,
        "std": 8.0,
        "min": 0.0,
        "max": 2000.0,
    },
    {
        "name": "Network Throughput Sensor",
        "type": "throughput",
        "unit": "mbps",
        "base": 85.0,
        "std": 20.0,
        "min": 0.0,
        "max": 1000.0,
    },
]


class NetworkSensor(BaseSensor):

    def __init__(self, gateway_id: str):
        super().__init__(gateway_id)
        self._spec = random.choice(SENSOR_SPECS)

    @property
    def sensor_type_name(self) -> str:
        return "net"

    @property
    def kafka_topic_key(self) -> str:
        return "sfc2_ingress"

    def _pick_reading(self) -> dict:
        value = self._spec["base"] + random.gauss(0, self._spec["std"])
        value = max(self._spec["min"], min(self._spec["max"], value))
        return {
            "name": self._spec["name"],
            "type": self._spec["type"],
            "unit": self._spec["unit"],
            "value": round(value, 4),
        }
