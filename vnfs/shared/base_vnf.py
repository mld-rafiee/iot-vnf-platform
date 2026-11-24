"""
base_vnf.py — Abstract base class for all VNFs.

Architecture:
  - Entry VNFs (FW1, FW2): consume from Kafka, forward via UDP
  - Middle VNFs (DPI, ENC, COMP, NAT): receive via UDP, forward via UDP or Kafka
  - Exit VNFs (COMP, NAT): receive via UDP, produce to Kafka egress topic

Throughput design:
  Uses multiprocessing (not threading) to bypass Python GIL.
  Each worker process gets its own CPU core, its own Kafka consumer,
  its own UDP socket — same pattern used in the IoT simulator.

  Entry VNFs (Kafka input):
    main process → spawn N worker processes
    each worker process: poll Kafka → process → UDP send

  Middle/exit VNFs (UDP input):
    main process → spawn N worker processes
    each worker: bind UDP port with SO_REUSEPORT → process → UDP/Kafka send

Per-hop SFC timestamping:
  Each VNF adds its own timestamp to the message under sfc_hops[].
  This enables end-to-end SFC chain delay calculation per ETSI TST001
  and PPTMon (Van Tu et al., IEEE TNSM 2021).
"""

import abc
import json
import logging
import multiprocessing
import os
import queue
import signal
import socket
import threading
import time
from collections import deque
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Optional

from confluent_kafka import Consumer, KafkaError, Producer
from prometheus_client import start_http_server

from metrics import VNFMetrics
from vnf_memory import VNFMemoryTracker

log = logging.getLogger(__name__)

# ── Tunable constants ──────────────────────────────────────────────────────────
NUM_WORKERS     = 4        # parallel worker processes (match CPU cores on worker node)
BATCH_SIZE      = 100      # UDP send batch size
BATCH_TIMEOUT   = 0.010    # 10ms batch flush timeout
METRICS_PORT    = 8000     # Prometheus scrape port
HEALTH_PORT     = 8010     # Kubernetes liveness probe port (above metrics ports)
REPORT_INTERVAL = 1.0      # metrics reporting interval (seconds)
UDP_BUFFER_SIZE = 65535    # UDP receive buffer


# ── Health check HTTP server ───────────────────────────────────────────────────

class _HealthHandler(BaseHTTPRequestHandler):
    """Minimal HTTP handler for Kubernetes liveness/readiness probes."""

    def do_GET(self):
        if self.path in ("/health", "/ready"):
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"ok")
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, format, *args):
        pass  # suppress access logs


def _run_health_server(port: int):
    """Run health server in its own thread."""
    server = HTTPServer(("0.0.0.0", port), _HealthHandler)
    server.serve_forever()


# ── Worker process entry point ─────────────────────────────────────────────────

def _worker_process(
    worker_id:  int,
    vnf_name:   str,
    config:     dict,
    vnf_class,          # the concrete VNF subclass
    stop_event: multiprocessing.Event,
):
    """
    One worker process. Runs on its own CPU core.
    Has its own Kafka consumer, UDP socket, metrics registry,
    and VNFMemoryTracker — no shared state with other workers.
    """
    logging.basicConfig(
        level=logging.INFO,
        format=f"%(asctime)s [%(levelname)s] {vnf_name}-w{worker_id} — %(message)s",
    )
    log = logging.getLogger(f"{vnf_name}.worker.{worker_id}")

    # Each worker gets its own metrics on a different port
    # (Prometheus will scrape all ports via kubernetes pod annotation)
    metrics_port = METRICS_PORT + worker_id
    metrics = VNFMetrics(vnf_name=f"{vnf_name}-w{worker_id}", port=metrics_port)
    metrics.start_server()

    mem = VNFMemoryTracker(name=f"{vnf_name}-w{worker_id}")

    transport = config["transport"]

    # ── Kafka consumer (entry VNFs only) ───────────────────────────────────────
    kafka_consumer = None
    if transport.get("kafka_input"):
        kafka_consumer = Consumer({
            "bootstrap.servers":       config["kafka"]["bootstrap_servers"],
            "group.id":                f"{vnf_name}-consumer-group",
            "auto.offset.reset":       "latest",
            "enable.auto.commit":      True,
            "auto.commit.interval.ms": 1000,
            "session.timeout.ms":      30000,
            "max.poll.interval.ms":    300000,
        })
        kafka_consumer.subscribe(transport["kafka_input_topics"])
        log.info(f"Kafka consumer subscribed to: {transport['kafka_input_topics']}")

    # ── Kafka producer (exit VNFs only) ───────────────────────────────────────
    kafka_producer = None
    kafka_output_topic = None
    if transport.get("kafka_output"):
        kafka_producer = Producer({
            "bootstrap.servers":             config["kafka"]["bootstrap_servers"],
            "acks":                          "1",
            "linger.ms":                     10,
            "batch.size":                    65536,
            "compression.type":              "lz4",
            "queue.buffering.max.messages":  100000,
        })
        kafka_output_topic = transport["kafka_output_topic"]

    # ── UDP input socket (middle/exit VNFs) ────────────────────────────────────
    udp_input = None
    if transport.get("udp_input"):
        udp_input = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        # SO_REUSEPORT lets multiple worker processes bind the same port
        # Each gets its own independent receive queue in the kernel
        # This is how real-world multi-process UDP servers work
        udp_input.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        udp_input.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        udp_input.bind(("0.0.0.0", transport["udp_input_port"]))
        udp_input.settimeout(1.0)
        log.info(f"UDP input bound to 0.0.0.0:{transport['udp_input_port']}")

    # ── UDP output socket (entry/middle VNFs) ──────────────────────────────────
    udp_output     = None
    udp_output_host = transport.get("udp_output_host")
    udp_output_port = transport.get("udp_output_port")
    if udp_output_host:
        udp_output = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    # ── UDP batch send queue ───────────────────────────────────────────────────
    send_queue      = deque()
    send_queue_lock = threading.Lock()

    def batch_send(data: bytes):
        with send_queue_lock:
            send_queue.append(data)
            if len(send_queue) >= BATCH_SIZE:
                _flush(send_queue)

    def _flush(q: deque):
        packets = list(q)
        q.clear()
        for pkt in packets:
            try:
                udp_output.sendto(pkt, (udp_output_host, udp_output_port))
            except Exception as e:
                log.error(f"UDP send error: {e}")

    def batch_sender_loop():
        while not stop_event.is_set():
            time.sleep(BATCH_TIMEOUT)
            with send_queue_lock:
                if send_queue:
                    _flush(send_queue)

    threading.Thread(target=batch_sender_loop, daemon=True).start()

    # ── Instantiate the concrete VNF (subclass) ────────────────────────────────
    vnf_instance = vnf_class(
        vnf_name = vnf_name,
        config   = config,
        metrics  = metrics,
        mem      = mem,
    )

    # ── Throughput + jitter tracking ───────────────────────────────────────────
    bytes_window      = 0
    bytes_window_lock = threading.Lock()
    window_start      = time.time()
    last_delay        = None   # for jitter = |D(n) - D(n-1)|
    report_time       = time.time()

    def process_one(raw_bytes: bytes):
        nonlocal bytes_window, last_delay

        try:
            message = json.loads(raw_bytes.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            log.error(f"Decode error: {e}")
            metrics.messages_dropped.labels(
                vnf=f"{vnf_name}-w{worker_id}", reason="decode_error"
            ).inc()
            return

        msg_size = len(raw_bytes)

        # ETSI IFA-027: ingress CP metrics
        metrics.bytes_ingress.labels(vnf=f"{vnf_name}-w{worker_id}").inc(msg_size)
        metrics.packets_ingress.labels(vnf=f"{vnf_name}-w{worker_id}").inc()

        # Memory tracking
        flow_key = vnf_instance.get_flow_key(message)
        mem.record_packet(raw_bytes, flow_key)

        # Per-hop SFC timestamp (ETSI TST001, PPTMon)
        # Records when this VNF received the message
        if "sfc_hops" not in message:
            message["sfc_hops"] = []
        hop_recv_time = time.time()
        message["sfc_hops"].append({
            "vnf":      vnf_name,
            "recv_ts":  hop_recv_time,
        })

        t_start = time.time()

        # VNF-specific processing
        result = vnf_instance.process(message)

        processing_time = time.time() - t_start

        if result is None:
            metrics.messages_dropped.labels(
                vnf=f"{vnf_name}-w{worker_id}", reason="filtered"
            ).inc()
            return

        # Update hop with send timestamp and processing delay
        message["sfc_hops"][-1]["send_ts"]          = time.time()
        message["sfc_hops"][-1]["processing_delay"] = processing_time

        # Processing delay histogram
        metrics.processing_delay.labels(
            vnf=f"{vnf_name}-w{worker_id}"
        ).observe(processing_time)

        # Jitter = |current_delay - previous_delay| (RFC 3550)
        if last_delay is not None:
            jitter = abs(processing_time - last_delay)
            metrics.packet_delay_variation.labels(
                vnf=f"{vnf_name}-w{worker_id}"
            ).observe(jitter)
        last_delay = processing_time

        # Serialize result
        out_bytes = json.dumps(result).encode("utf-8")
        out_size  = len(out_bytes)

        # Forward to next hop
        if kafka_producer:
            kafka_producer.produce(
                kafka_output_topic,
                key=result.get("sensor_id", "").encode("utf-8"),
                value=out_bytes,
            )
            kafka_producer.poll(0)
        elif udp_output:
            batch_send(out_bytes)

        # Egress metrics
        metrics.bytes_egress.labels(
            vnf=f"{vnf_name}-w{worker_id}"
        ).inc(out_size)
        metrics.packets_egress.labels(
            vnf=f"{vnf_name}-w{worker_id}"
        ).inc()
        metrics.messages_processed.labels(
            vnf=f"{vnf_name}-w{worker_id}"
        ).inc()

        with bytes_window_lock:
            bytes_window += out_size

    # ── Background metrics reporting ───────────────────────────────────────────
    def metrics_loop():
        nonlocal bytes_window, window_start, report_time
        while not stop_event.is_set():
            time.sleep(REPORT_INTERVAL)
            now     = time.time()
            elapsed = now - window_start

            # Throughput Mbps
            with bytes_window_lock:
                snap         = bytes_window
                bytes_window = 0
            mbps = (snap * 8) / (elapsed * 1_000_000) \
                if elapsed > 0 else 0.0
            metrics.throughput_mbps.labels(
                vnf=f"{vnf_name}-w{worker_id}"
            ).set(mbps)
            window_start = now

            # CPU utilization (self-reported)
            try:
                import resource
                usage = resource.getrusage(resource.RUSAGE_SELF)
                cpu_elapsed = now - report_time
                cpu_used = usage.ru_utime + usage.ru_stime
                metrics.cpu_utilization.labels(
                    vnf=f"{vnf_name}-w{worker_id}"
                ).set(min(100.0, (cpu_used / cpu_elapsed) * 100))
            except Exception:
                pass
            report_time = now

            # Uptime + memory
            metrics.update_uptime()
            metrics.update_memory_stats(mem)

            log.info(
                f"throughput: {mbps:.2f} Mbps | "
                f"flows: {mem.stats()['active_flows']}"
            )

    threading.Thread(target=metrics_loop, daemon=True).start()

    # ── Main receive loop ──────────────────────────────────────────────────────
    log.info(f"Worker started (PID {os.getpid()})")

    if kafka_consumer:
        # Entry VNF — poll Kafka
        while not stop_event.is_set():
            msg = kafka_consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    log.error(f"Kafka error: {msg.error()}")
                continue
            process_one(msg.value())
        kafka_consumer.close()

    elif udp_input:
        # Middle/exit VNF — receive UDP
        while not stop_event.is_set():
            try:
                data, _ = udp_input.recvfrom(UDP_BUFFER_SIZE)
                process_one(data)
            except socket.timeout:
                continue
            except Exception as e:
                log.error(f"UDP receive error: {e}")

    if kafka_producer:
        kafka_producer.flush()

    log.info(f"Worker stopped (PID {os.getpid()})")


# ── Base VNF ───────────────────────────────────────────────────────────────────

class BaseVNF(abc.ABC):
    """
    Abstract base for all VNFs.

    Subclasses must implement:
      process(message: dict) -> Optional[dict]
        Receives the decoded JSON message.
        Returns the processed message to forward, or None to drop it.

    Subclasses may override:
      get_flow_key(message: dict) -> tuple
        Returns a hashable tuple for memory tracking.
    """

    def __init__(
        self,
        vnf_name: str,
        config:   dict,
        metrics:  VNFMetrics  = None,
        mem:      VNFMemoryTracker = None,
    ):
        self.vnf_name = vnf_name
        self.config   = config
        self.metrics  = metrics   # injected by worker process
        self.mem      = mem       # injected by worker process

    @abc.abstractmethod
    def process(self, message: dict) -> Optional[dict]:
        """
        Process one message.
        Return processed message to forward, or None to drop.
        """

    def get_flow_key(self, message: dict) -> tuple:
        return (
            message.get("gateway_id", "unknown"),
            message.get("sfc_target", "unknown"),
            message.get("type", "unknown"),
        )

    @classmethod
    def run(cls, vnf_name: str, config: dict):
        """
        Start the VNF — called from main.py of each VNF.
        Spawns NUM_WORKERS processes and starts health server.
        """
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
        )
        log = logging.getLogger(vnf_name)
        log.info(f"[{vnf_name}] Starting with {NUM_WORKERS} worker processes")

        # Health server in main process
        threading.Thread(
            target=_run_health_server,
            args=(HEALTH_PORT,),
            daemon=True,
        ).start()

        stop_event = multiprocessing.Event()

        # Handle SIGTERM gracefully (Kubernetes sends this on pod shutdown)
        def _handle_signal(sig, frame):
            log.info(f"[{vnf_name}] Received signal {sig}, stopping...")
            stop_event.set()

        signal.signal(signal.SIGTERM, _handle_signal)
        signal.signal(signal.SIGINT,  _handle_signal)

        # Spawn worker processes
        workers = []
        for i in range(NUM_WORKERS):
            p = multiprocessing.Process(
                target=_worker_process,
                args=(i, vnf_name, config, cls, stop_event),
                name=f"{vnf_name}-worker-{i}",
            )
            p.start()
            log.info(f"[{vnf_name}] Worker {i} started (PID {p.pid})")
            workers.append(p)

        # Wait for all workers
        for p in workers:
            p.join()

        log.info(f"[{vnf_name}] All workers stopped")
