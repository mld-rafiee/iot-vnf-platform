"""
IoT Gateway — one instance per software gateway.

Responsibilities:
  1. Create N IoT devices (mixed types)
  2. Run each device in a thread at its configured rate
  3. Buffer all messages in a shared deque
  4. Batch messages (batch_size from config)
  5. Rate-limit sends using NNE 4G/5G speeds
  6. Send batches to Kafka topics
"""
import collections
import json
import logging
import random
import threading
import time
import uuid

from kafka import KafkaProducer
from kafka.errors import KafkaError

from devices import EnvironmentalSensor, IndustrialSensor, NetworkSensor
from nne import NNEBandwidthShaper

log = logging.getLogger(__name__)

# Internal buffer limit — not user-configurable (memory protection)
_BUFFER_SIZE = 1_000_000

# Sensor type distribution: 40% env, 30% industrial, 30% network
# Matches real IoT deployments (more environmental in smart city/building)
_SENSOR_DISTRIBUTION = [
    (EnvironmentalSensor, 0.4),
    (IndustrialSensor, 0.3),
    (NetworkSensor, 0.3),
]


def _create_devices(n: int, gateway_id: str, rates: list) -> list:
    """Create N devices with realistic type distribution."""
    devices = []
    for _ in range(n):
        # Pick type based on distribution weights
        rand = random.random()
        cumulative = 0
        chosen_class = EnvironmentalSensor
        for cls, weight in _SENSOR_DISTRIBUTION:
            cumulative += weight
            if rand <= cumulative:
                chosen_class = cls
                break
        sensor = chosen_class(gateway_id)
        sensor.rate = random.choice(rates)
        devices.append(sensor)
    return devices


def _device_worker(sensor, buffer: collections.deque, stop_event: threading.Event):
    """
    Thread worker for one IoT device.
    Generates messages at sensor.rate messages/second.
    """
    interval = 1.0 / sensor.rate
    while not stop_event.is_set():
        message = sensor.generate()
        if len(buffer) >= _BUFFER_SIZE:
            buffer.popleft()  # Drop oldest if full
        buffer.append(message)
        time.sleep(interval)


class Gateway:
    """
    One software IoT gateway managing N devices.
    Mirrors the architecture from your paper exactly.
    """

    def __init__(self, gateway_id: str, num_devices: int, config: dict):
        self.gateway_id = gateway_id
        self.num_devices = num_devices
        self.config = config

        self._buffer = collections.deque()
        self._stop_event = threading.Event()
        self._threads = []

        # Kafka producer
        self._producer = self._create_producer()

        # NNE bandwidth shaper
        nne_cfg = config["nne"]
        self._shaper = NNEBandwidthShaper(
            csv_path=nne_cfg["csv_path"],
            rate_column=nne_cfg["rate_column"],
            update_interval_seconds=nne_cfg["update_interval_seconds"],
        )

        # Topics
        self._topics = config["kafka"]["topics"]
        self._batch_size = config["sensor"]["batch_size"]
        self._rates = config["sensor"]["rates_per_second"]
        self._report_interval = config["logging"]["report_interval_seconds"]

    def _create_producer(self) -> KafkaProducer:
        bootstrap = self.config["kafka"]["bootstrap_servers"]
        log.info(f"[{self.gateway_id}] Connecting to Kafka at {bootstrap}")
        return KafkaProducer(
            bootstrap_servers=bootstrap,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8"),
            acks=1,                   # Wait for leader acknowledgment
            retries=3,
            linger_ms=5,              # Small batching delay for efficiency
        )

    def start(self):
        """Start all device threads and the sender loop."""
        log.info(
            f"[{self.gateway_id}] Starting {self.num_devices} devices"
        )

        # Create and start device threads
        devices = _create_devices(
            self.num_devices, self.gateway_id, self._rates
        )
        for sensor in devices:
            t = threading.Thread(
                target=_device_worker,
                args=(sensor, self._buffer, self._stop_event),
                daemon=True,
            )
            t.start()
            self._threads.append(t)

        # Run the send loop in the main gateway thread
        self._send_loop()

    def stop(self):
        """Signal all threads to stop."""
        log.info(f"[{self.gateway_id}] Stopping...")
        self._stop_event.set()

    def _send_loop(self):
        """
        Main loop:
        1. Wait for batch_size messages in buffer
        2. Get current NNE rate
        3. Send batch to Kafka at that rate
        4. Report throughput every report_interval seconds
        """
        total_bytes_sent = 0
        last_report_time = time.time()

        while not self._stop_event.is_set():
            # Wait until we have a full batch
            if len(self._buffer) < self._batch_size:
                time.sleep(0.01)
                continue

            # Pull one batch from buffer
            batch = []
            for _ in range(self._batch_size):
                if self._buffer:
                    batch.append(self._buffer.popleft())

            if not batch:
                continue

            # Get current 4G/5G rate from NNE
            rate_bytes_per_sec = self._shaper.current_rate_bytes_per_sec()

            # Send messages to Kafka with rate limiting
            batch_bytes = 0
            for msg in batch:
                topic_key = msg.get("sfc_target")
                if topic_key == "sfc1":
                    topic = self._topics["sfc1_ingress"]
                else:
                    topic = self._topics["sfc2_ingress"]

                msg_bytes = json.dumps(msg).encode("utf-8")
                batch_bytes += len(msg_bytes)

                try:
                    self._producer.send(
                        topic,
                        key=msg["sensor_id"],
                        value=msg,
                    )
                    # Also send everything to raw topic (data lake)
                    self._producer.send(
                        self._topics["raw"],
                        key=msg["sensor_id"],
                        value=msg,
                    )
                except KafkaError as e:
                    log.error(f"[{self.gateway_id}] Kafka error: {e}")

            # Flush the producer
            self._producer.flush()

            # Rate limiting: sleep based on NNE bandwidth
            if rate_bytes_per_sec > 0:
                sleep_time = batch_bytes / rate_bytes_per_sec
                time.sleep(sleep_time * 0.2)  # 20% sleep (match original code)

            total_bytes_sent += batch_bytes

            # Report transmission rate
            now = time.time()
            elapsed = now - last_report_time
            if elapsed >= self._report_interval:
                rate_mbps = (total_bytes_sent * 8) / (elapsed * 1_000_000)
                nne_mbps = self._shaper.current_rate_mbps()
                log.info(
                    f"[{self.gateway_id}] "
                    f"TX rate: {rate_mbps:.2f} Mbps | "
                    f"NNE target: {nne_mbps:.2f} Mbps | "
                    f"Buffer: {len(self._buffer)}"
                )
                total_bytes_sent = 0
                last_report_time = now
