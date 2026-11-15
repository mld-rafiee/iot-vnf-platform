"""
IoT Gateway — one instance per software gateway.

Uses multiprocessing to bypass Python GIL:
each producer process gets its own CPU core.
"""
import collections
import json
import logging
import multiprocessing
import random
import threading
import time

from confluent_kafka import Producer as ConfluentProducer

from devices import EnvironmentalSensor, IndustrialSensor, NetworkSensor
from nne import NNEBandwidthShaper

log = logging.getLogger(__name__)

_BUFFER_SIZE = 1_000_000

_SENSOR_DISTRIBUTION = [
    (EnvironmentalSensor, 0.4),
    (IndustrialSensor, 0.3),
    (NetworkSensor, 0.3),
]

# Number of parallel producer processes per gateway
# Each gets its own CPU core — bypasses GIL
_NUM_PRODUCERS = 4


def _create_devices(n: int, gateway_id: str, rates: list) -> list:
    devices = []
    for _ in range(n):
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


def _device_worker(sensor, queue: multiprocessing.Queue, stop_event):
    interval = 1.0 / sensor.rate
    while not stop_event.is_set():
        message = sensor.generate()
        try:
            queue.put_nowait(message)
        except Exception:
            pass  # Drop if queue full
        time.sleep(interval)


def _producer_worker(
    worker_id: int,
    queue: multiprocessing.Queue,
    config: dict,
    stop_event,
):
    """
    One producer process — runs on its own CPU core.
    Reads from shared queue, sends to Kafka.
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    )
    log = logging.getLogger(f"producer-{worker_id}")

    bootstrap = config["kafka"]["bootstrap_servers"]
    topics = config["kafka"]["topics"]
    batch_size = config["sensor"]["batch_size"]
    report_interval = config["logging"]["report_interval_seconds"]

    nne_cfg = config["nne"]
    shaper = NNEBandwidthShaper(
        csv_path=nne_cfg["csv_path"],
        rate_column=nne_cfg["rate_column"],
        update_interval_seconds=nne_cfg["update_interval_seconds"],
    )

    producer = ConfluentProducer({
        "bootstrap.servers": bootstrap,
        "acks": "1",
        "linger.ms": 50,
        "batch.size": 131072,
        "compression.type": "lz4",
        "queue.buffering.max.messages": 500000,
        "queue.buffering.max.kbytes": 524288,
    })

    total_bytes = 0
    last_report = time.time()
    window_bytes = 0
    window_start = time.time()

    while not stop_event.is_set():
        batch = []
        try:
            while len(batch) < batch_size:
                try:
                    msg = queue.get_nowait()
                    batch.append(msg)
                except Exception:
                    break
        except Exception:
            pass

        if not batch:
            time.sleep(0.005)
            continue

        rate_bps = shaper.current_rate_bytes_per_sec() / _NUM_PRODUCERS
        batch_bytes = 0

        for msg in batch:
            topic_key = msg.get("sfc_target")
            topic = topics["sfc1_ingress"] if topic_key == "sfc1" \
                else topics["sfc2_ingress"]

            msg_value = json.dumps(msg).encode("utf-8")
            msg_key = msg["sensor_id"].encode("utf-8")
            batch_bytes += len(msg_value)

            try:
                producer.produce(topic, key=msg_key, value=msg_value)
                producer.produce(topics["raw"], key=msg_key, value=msg_value)
            except Exception as e:
                log.error(f"Kafka error: {e}")

        producer.poll(0)

        # Token bucket rate limiting
        window_bytes += batch_bytes
        window_elapsed = time.time() - window_start
        if window_elapsed > 0:
            current_rate = window_bytes / window_elapsed
            if current_rate > rate_bps:
                sleep_time = (window_bytes / rate_bps) - window_elapsed
                if sleep_time > 0:
                    time.sleep(sleep_time)
        if time.time() - window_start >= 1.0:
            window_bytes = 0
            window_start = time.time()

        total_bytes += batch_bytes
        now = time.time()
        elapsed = now - last_report
        if elapsed >= report_interval:
            rate_mbps = (total_bytes * 8) / (elapsed * 1_000_000)
            nne_mbps = shaper.current_rate_mbps()
            log.info(
                f"TX rate: {rate_mbps:.2f} Mbps | "
                f"NNE target: {nne_mbps:.2f} Mbps | "
                f"Queue: {queue.qsize()}"
            )
            total_bytes = 0
            last_report = now

    producer.flush()


class Gateway:

    def __init__(self, gateway_id: str, num_devices: int, config: dict):
        self.gateway_id = gateway_id
        self.num_devices = num_devices
        self.config = config
        self._rates = config["sensor"]["rates_per_second"]

    def start(self):
        log.info(
            f"[{self.gateway_id}] Starting {self.num_devices} devices "
            f"with {_NUM_PRODUCERS} producer processes"
        )

        # Shared queue between device threads and producer processes
        queue = multiprocessing.Queue(maxsize=_BUFFER_SIZE)
        stop_event = multiprocessing.Event()

        # Start producer processes (each on its own CPU core)
        producers = []
        for i in range(_NUM_PRODUCERS):
            p = multiprocessing.Process(
                target=_producer_worker,
                args=(i, queue, self.config, stop_event),
            )
            p.start()
            producers.append(p)
            log.info(f"[{self.gateway_id}] Producer process {i} started (PID {p.pid})")

        # Start device threads (share the queue)
        devices = _create_devices(
            self.num_devices, self.gateway_id, self._rates
        )
        threads = []
        thread_stop = threading.Event()
        for sensor in devices:
            t = threading.Thread(
                target=_device_worker,
                args=(sensor, queue, thread_stop),
                daemon=True,
            )
            t.start()
            threads.append(t)

        log.info(f"[{self.gateway_id}] All {len(devices)} devices started")

        try:
            # Keep main process alive
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            log.info(f"[{self.gateway_id}] Stopping...")
            thread_stop.set()
            stop_event.set()
            for p in producers:
                p.join(timeout=5)
