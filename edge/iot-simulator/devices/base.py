"""
Base sensor class — all IoT device types inherit from this.
"""
import uuid
import random
from abc import ABC, abstractmethod
from datetime import datetime, timezone


class BaseSensor(ABC):
    """
    Abstract base class for all IoT sensors.
    Every sensor has an ID, generates readings,
    and targets a specific Kafka topic.
    """

    def __init__(self, gateway_id: str):
        self.sensor_id = str(uuid.uuid4())[:8]
        self.gateway_id = gateway_id
        # Each device picks a random rate from [4, 12, 25, 50]
        # This is set by the gateway from config
        self.rate = None

    @property
    @abstractmethod
    def sensor_type_name(self) -> str:
        """Human-readable sensor category."""
        pass

    @property
    @abstractmethod
    def kafka_topic_key(self) -> str:
        """Which topic key this sensor sends to: 'sfc1_ingress' or 'sfc2_ingress'."""
        pass

    @abstractmethod
    def _pick_reading(self) -> dict:
        """
        Return a dict with: name, type, unit, value.
        Subclasses implement this.
        """
        pass

    def generate(self) -> dict:
        """
        Generate one sensor message in the standard JSON format.
        Matches the format from your paper exactly.
        """
        reading = self._pick_reading()
        return {
            "sensor_id": f"{self.sensor_type_name[:3]}-{self.gateway_id}-{self.sensor_id}-{reading['type']}",
            "name": reading["name"],
            "type": reading["type"],
            "unit": reading["unit"],
            "value": reading["value"],
            "timestamp": datetime.now(timezone.utc).timestamp(),
            "gateway_id": self.gateway_id,
            "sfc_target": "sfc1" if self.kafka_topic_key == "sfc1_ingress" else "sfc2",
        }
