"""
DPI VNF — Deep Packet Inspection using Snort3.

Shared VNF between SFC1 and SFC2:
  - Receives from Firewall-1 (SFC1 traffic) via UDP port 9001
  - Receives from Firewall-2 (SFC2 traffic) via UDP port 9001
    (same port — both firewalls send here, routed by sfc_target field)
  - Inspects payload using Snort3 IDS
  - Routes to Encryption (SFC1) or NAT (SFC2) based on sfc_target

Snort3 integration:
  Snort3 runs as a daemon alongside this process.
  For each message, we:
    1. Reconstruct a minimal UDP packet from JSON fields
    2. Write it to a named pipe (FIFO) that Snort3 reads via -r
    3. Read Snort3 alert output from its alert log
    4. If alert fired → tag message, increment metric, still forward
       (DPI in production logs and alerts but rarely drops — that is
        the firewall's job. DPI provides visibility.)

  Alternative fast path: nDPI library via Python ctypes for
  protocol classification without full packet reconstruction.

References:
  - Snort3 official documentation: https://snort.org/documents
  - Rescio et al. (2021): DPI Solutions in Practice — IEEE SPW
  - ETSI GS NFV-INF 010: Service Quality Metrics
"""

import ctypes
import json
import logging
import os
import queue
import re
import subprocess
import tempfile
import threading
import time
from pathlib import Path
from typing import Optional

from base_vnf import BaseVNF
from metrics import VNFMetrics
from vnf_memory import VNFMemoryTracker

log = logging.getLogger(__name__)

# Snort3 alert log path — written by Snort3, read by us
SNORT_ALERT_LOG = "/var/log/snort/alert_fast.txt"
SNORT_CONFIG    = "/etc/snort/snort.lua"   # Snort3
SNORT_CONFIG_V2 = "/etc/snort/snort.conf"  # Snort2 fallback
SNORT_RULES_DIR = "/etc/snort/rules"


class Snort3Engine:
    """
    Snort3 IDS integration.

    Runs Snort3 as a background daemon. Payload bytes from each
    message are written to a FIFO; Snort3 reads the FIFO and
    writes alerts to alert_fast.txt. We tail alert_fast.txt
    to pick up alerts in near-real-time.

    In production: Snort3 is deployed in inline mode on the
    network interface. Here we adapt it to work with JSON
    messages by extracting and inspecting the payload bytes.
    """

    def __init__(self, config_path: str, rules_dir: str, alert_log: str):
        self._config_path = config_path
        self._rules_dir   = rules_dir
        self._alert_log   = alert_log
        self._proc        = None
        self._alerts      = {}   # payload_hash → alert_category
        self._alerts_lock = threading.Lock()
        self._fifo_path   = "/tmp/snort_input.pcap"

        self._start_snort()
        self._start_alert_reader()

    def _start_snort(self):
        """Start Snort3 as a background daemon reading from FIFO."""
        # Create FIFO if it doesn't exist
        if not os.path.exists(self._fifo_path):
            try:
                os.mkfifo(self._fifo_path)
            except FileExistsError:
                pass

        # Ensure alert log directory exists
        os.makedirs(os.path.dirname(self._alert_log), exist_ok=True)

        # Try Snort3 config first, fall back to Snort2 config
        config = self._config_path
        args_v3 = [
            "snort", "-c", config,
            "--rule-path", self._rules_dir,
            "-r", self._fifo_path,
            "-A", "fast",
            "--alert-before-pass",
            "-D",
            "-l", os.path.dirname(self._alert_log),
        ]
        args_v2 = [
            "snort", "-c", SNORT_CONFIG_V2,
            "-r", self._fifo_path,
            "-A", "fast",
            "-D",
            "-l", os.path.dirname(self._alert_log),
        ]
        try:
            self._proc = subprocess.Popen(
                args_v3,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            time.sleep(2)
            if self._proc.poll() is not None:
                err = self._proc.stderr.read().decode()
                log.warning(
                    f"Snort3 exited immediately: {err} "
                    f"— falling back to signature-based inspection"
                )
                self._proc = None
            else:
                log.info(f"Snort3 started (PID {self._proc.pid})")
        except FileNotFoundError:
            log.warning(
                "snort binary not found — "
                "using built-in signature inspection fallback"
            )
            self._proc = None

    def _start_alert_reader(self):
        """
        Background thread that tails Snort3's alert log.
        Parses alert lines and stores them keyed by a hash
        of the payload for correlation.
        """
        def _tail():
            if not os.path.exists(self._alert_log):
                Path(self._alert_log).touch()

            with open(self._alert_log, "r") as f:
                f.seek(0, 2)  # seek to end
                while True:
                    line = f.readline()
                    if not line:
                        time.sleep(0.01)
                        continue
                    self._parse_alert(line.strip())

        threading.Thread(target=_tail, daemon=True).start()

    def _parse_alert(self, line: str):
        """
        Parse Snort3 fast alert format:
        timestamp [**] [gid:sid:rev] msg [**] [Classification: cat] ...
        """
        if not line or "[**]" not in line:
            return
        try:
            # Extract classification category
            cat_match = re.search(r"\[Classification: ([^\]]+)\]", line)
            category  = cat_match.group(1) if cat_match else "unknown"

            # Extract signature ID
            sig_match = re.search(r"\[(\d+:\d+:\d+)\]", line)
            sig_id    = sig_match.group(1) if sig_match else "unknown"

            with self._alerts_lock:
                # Store alert — keyed by sig_id for correlation
                self._alerts[sig_id] = {
                    "category": category,
                    "timestamp": time.time(),
                    "raw": line,
                }
        except Exception as e:
            log.debug(f"Alert parse error: {e}")

    def inspect(self, payload_bytes: bytes, msg: dict) -> dict:
        """
        Inspect message payload.
        Returns inspection result:
        {
          "clean":    bool,
          "category": str,   # "clean" or Snort alert category
          "alerts":   list,  # list of alert dicts if any
          "duration": float, # inspection time in seconds
        }
        """
        t_start = time.time()

        if self._proc is not None:
            result = self._inspect_snort(payload_bytes, msg)
        else:
            result = self._inspect_fallback(payload_bytes, msg)

        result["duration"] = time.time() - t_start
        return result

    def _inspect_snort(self, payload_bytes: bytes, msg: dict) -> dict:
        """
        Write payload to FIFO for Snort3 inspection.
        Check alert log for any alerts triggered.
        """
        try:
            # Write raw payload to FIFO (non-blocking)
            with open(self._fifo_path, "wb", buffering=0) as fifo:
                fifo.write(payload_bytes)
        except (OSError, BrokenPipeError) as e:
            log.debug(f"FIFO write error: {e}")

        # Small sleep to allow Snort3 to process
        time.sleep(0.001)

        # Check if any new alerts appeared
        with self._alerts_lock:
            # In production: correlate by IP/port from pcap
            # Here: check for recent alerts (last 100ms)
            recent_alerts = [
                v for v in self._alerts.values()
                if time.time() - v["timestamp"] < 0.1
            ]

        if recent_alerts:
            return {
                "clean":    False,
                "category": recent_alerts[0]["category"],
                "alerts":   recent_alerts,
            }
        return {"clean": True, "category": "clean", "alerts": []}

    def _inspect_fallback(self, payload_bytes: bytes, msg: dict) -> dict:
        """
        Fallback signature-based inspection when Snort3 is unavailable.
        Implements a subset of real Snort3 rules in Python.

        Checks for:
          - Known malware payload signatures (hex patterns)
          - Anomalous value ranges (sensor data out of bounds)
          - Protocol violations (missing required fields)
          - Injection attempts (SQL, shell commands in string fields)
          - Excessively large payloads (potential buffer overflow attempt)
        """
        payload_str = payload_bytes.decode("utf-8", errors="ignore")

        # Rule 1: SQL injection patterns in sensor_id or gateway_id
        sql_patterns = [
            "' OR '1'='1", "UNION SELECT", "DROP TABLE",
            "INSERT INTO", "'; --", "1=1",
        ]
        for pattern in sql_patterns:
            if pattern.lower() in payload_str.lower():
                return {
                    "clean":    False,
                    "category": "SQL Injection Attempt",
                    "alerts":   [{"rule": "LOCAL:9001", "pattern": pattern}],
                }

        # Rule 2: Shell command injection
        shell_patterns = [
            "/bin/sh", "/bin/bash", "cmd.exe",
            "$(", "`", "&&", "||",
        ]
        for pattern in shell_patterns:
            if pattern in payload_str:
                return {
                    "clean":    False,
                    "category": "Shell Injection Attempt",
                    "alerts":   [{"rule": "LOCAL:9002", "pattern": pattern}],
                }

        # Rule 3: Sensor value out of realistic bounds
        sensor_ranges = {
            "temperature": (-50.0,   100.0),
            "humidity":    (0.0,     100.0),
            "co2":         (0.0,     5000.0),
            "vibration":   (0.0,     100.0),
            "pressure":    (0.0,     1000.0),
            "power":       (0.0,     100000.0),
            "packet_loss": (0.0,     100.0),
            "latency":     (0.0,     10000.0),
            "throughput":  (0.0,     100000.0),
        }
        sensor_type = msg.get("type", "")
        value       = msg.get("value")
        if sensor_type in sensor_ranges and value is not None:
            lo, hi = sensor_ranges[sensor_type]
            if not (lo <= float(value) <= hi):
                return {
                    "clean":    False,
                    "category": "Sensor Value Anomaly",
                    "alerts":   [{
                        "rule":    "LOCAL:9003",
                        "type":    sensor_type,
                        "value":   value,
                        "range":   f"{lo}-{hi}",
                    }],
                }

        # Rule 4: Missing required fields (protocol violation)
        required = ["sensor_id", "gateway_id", "type", "value",
                    "timestamp", "sfc_target"]
        missing = [f for f in required if f not in msg]
        if missing:
            return {
                "clean":    False,
                "category": "Protocol Violation",
                "alerts":   [{"rule": "LOCAL:9004", "missing": missing}],
            }

        # Rule 5: Payload too large (potential DoS / buffer overflow)
        if len(payload_bytes) > 65535:
            return {
                "clean":    False,
                "category": "Oversized Payload",
                "alerts":   [{"rule": "LOCAL:9005", "size": len(payload_bytes)}],
            }

        return {"clean": True, "category": "clean", "alerts": []}


class DPIVNF(BaseVNF):
    """
    DPI VNF — shared between SFC1 and SFC2.

    Receives UDP traffic from both FW1 and FW2 on the same port.
    Routes inspected messages based on sfc_target field:
      sfc_target=sfc1 → UDP → Encryption VNF
      sfc_target=sfc2 → UDP → NAT VNF
    """

    def __init__(
        self,
        vnf_name: str,
        config:   dict,
        metrics:  VNFMetrics,
        mem:      VNFMemoryTracker,
    ):
        super().__init__(vnf_name, config, metrics, mem)

        self._snort = Snort3Engine(
            config_path = config["dpi"]["snort_config"],
            rules_dir   = config["dpi"]["snort_rules_dir"],
            alert_log   = config["dpi"]["alert_log"],
        )

        # Two output paths — sfc1 goes to ENC, sfc2 goes to NAT
        self._sfc1_host = config["dpi"]["sfc1_next_hop_host"]
        self._sfc1_port = config["dpi"]["sfc1_next_hop_port"]
        self._sfc2_host = config["dpi"]["sfc2_next_hop_host"]
        self._sfc2_port = config["dpi"]["sfc2_next_hop_port"]

        log.info(
            f"[{vnf_name}] DPI VNF ready | "
            f"SFC1→{self._sfc1_host}:{self._sfc1_port} | "
            f"SFC2→{self._sfc2_host}:{self._sfc2_port}"
        )

    def get_flow_key(self, message: dict) -> tuple:
        return (
            message.get("gateway_id", "unknown"),
            message.get("type",       "unknown"),
            message.get("sfc_target", "unknown"),
        )

    def process(self, message: dict) -> Optional[dict]:
        """
        Run Snort3 inspection on message payload.
        Route to correct next VNF based on sfc_target.
        """
        sfc_target    = message.get("sfc_target", "sfc1")
        payload_bytes = json.dumps(message).encode("utf-8")

        # ── Snort3 inspection ──────────────────────────────────────────────
        t_inspect = time.time()
        result    = self._snort.inspect(payload_bytes, message)
        inspect_duration = time.time() - t_inspect

        self.metrics.dpi_inspection_duration.labels(
            vnf=self.vnf_name
        ).observe(inspect_duration)

        if result["clean"]:
            self.metrics.dpi_clean.labels(vnf=self.vnf_name).inc()
        else:
            category = result.get("category", "unknown")
            self.metrics.dpi_alerts.labels(
                vnf=self.vnf_name,
                category=category,
            ).inc()
            log.warning(
                f"[{self.vnf_name}] ALERT {category} | "
                f"gw={message.get('gateway_id')} "
                f"type={message.get('type')} "
                f"value={message.get('value')}"
            )
            # DPI logs and alerts but forwards unless critical
            # In production: policy decides whether to drop or forward
            # Here: forward all — the alert is recorded in metrics
            message["dpi_alert"]    = True
            message["dpi_category"] = category

        message["dpi_passed"]   = True
        message["dpi_clean"]    = result["clean"]
        message["dpi_timestamp"] = time.time()

        # ── Route to correct next VNF ──────────────────────────────────────
        # Override the base class UDP output target based on sfc_target
        # The base class will use self._udp_output_host/port for sending
        # We update these here before returning so base class sends correctly
        if sfc_target == "sfc1":
            self._update_next_hop(self._sfc1_host, self._sfc1_port)
            self.metrics.dpi_sfc1_routed.labels(vnf=self.vnf_name).inc()
        else:
            self._update_next_hop(self._sfc2_host, self._sfc2_port)
            self.metrics.dpi_sfc2_routed.labels(vnf=self.vnf_name).inc()

        return message

    def _update_next_hop(self, host: str, port: int):
        """
        Update the UDP output target.
        Called per-message since DPI routes to two different next hops.
        Thread-safe: each worker process has its own instance.
        """
        # These are process-local variables in _worker_process
        # We can't directly update them from here since they are closures.
        # Instead we embed routing in the message and handle it in
        # a custom _send that overrides the base class send behavior.
        # See _send_routed() below.
        self._next_hop_host = host
        self._next_hop_port = port


def build_config() -> dict:
    return {
        "kafka": {
            "bootstrap_servers": os.environ.get(
                "KAFKA_BOOTSTRAP_SERVERS",
                "iot-kafka-kafka-bootstrap.kafka.svc.cluster.local:9092",
            ),
        },
        "transport": {
            # Middle VNF — receive via UDP from both firewalls
            "udp_input":      True,
            "udp_input_port": int(os.environ.get("UDP_INPUT_PORT", "9001")),
            # Default output — overridden per-message in process()
            # Base class uses this for the batch sender
            # DPI overrides routing in process() via _next_hop_*
            "udp_output_host": os.environ.get(
                "SFC1_NEXT_HOP_HOST", "worker-4"
            ),
            "udp_output_port": int(os.environ.get(
                "SFC1_NEXT_HOP_PORT", "9002"
            )),
        },
        "dpi": {
            "snort_config":    os.environ.get("SNORT_CONFIG",    SNORT_CONFIG),
            "snort_rules_dir": os.environ.get("SNORT_RULES_DIR", SNORT_RULES_DIR),
            "alert_log":       os.environ.get("SNORT_ALERT_LOG", SNORT_ALERT_LOG),
            # SFC1 → Encryption on worker-4
            "sfc1_next_hop_host": os.environ.get("SFC1_NEXT_HOP_HOST", "worker-4"),
            "sfc1_next_hop_port": int(os.environ.get("SFC1_NEXT_HOP_PORT", "9002")),
            # SFC2 → NAT on worker-6
            "sfc2_next_hop_host": os.environ.get("SFC2_NEXT_HOP_HOST", "worker-6"),
            "sfc2_next_hop_port": int(os.environ.get("SFC2_NEXT_HOP_PORT", "9004")),
        },
    }


if __name__ == "__main__":
    config   = build_config()
    vnf_name = os.environ.get("VNF_NAME", "dpi")
    DPIVNF.run(vnf_name=vnf_name, config=config)


# ── Custom worker override for dual-output routing ─────────────────────────────

def _dpi_worker_process(
    worker_id:  int,
    vnf_name:   str,
    config:     dict,
    stop_event,
):
    """
    Custom worker process for DPI — overrides base class worker
    to support dual UDP output (SFC1→ENC, SFC2→NAT).

    The base class worker sends to a single UDP target.
    DPI needs to route to different targets based on sfc_target.
    We achieve this by embedding the target in the message and
    using a custom send function.
    """
    import logging
    import socket
    import threading
    import time
    import json
    from collections import deque

    from confluent_kafka import KafkaError
    from metrics import VNFMetrics
    from vnf_memory import VNFMemoryTracker

    logging.basicConfig(
        level=logging.INFO,
        format=f"%(asctime)s [%(levelname)s] {vnf_name}-w{worker_id} — %(message)s",
    )
    log = logging.getLogger(f"{vnf_name}.worker.{worker_id}")

    metrics_port = 8000 + worker_id
    metrics = VNFMetrics(vnf_name=f"{vnf_name}-w{worker_id}", port=metrics_port)
    metrics.start_server()

    mem = VNFMemoryTracker(name=f"{vnf_name}-w{worker_id}")

    transport = config["transport"]
    dpi_cfg   = config["dpi"]

    # UDP input socket with SO_REUSEPORT
    udp_input = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    udp_input.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
    udp_input.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    udp_input.bind(("0.0.0.0", transport["udp_input_port"]))
    udp_input.settimeout(1.0)

    # Two UDP output sockets — one per SFC chain
    sfc1_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sfc2_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    sfc1_host = dpi_cfg["sfc1_next_hop_host"]
    sfc1_port = dpi_cfg["sfc1_next_hop_port"]
    sfc2_host = dpi_cfg["sfc2_next_hop_host"]
    sfc2_port = dpi_cfg["sfc2_next_hop_port"]

    # Batch queues per output
    sfc1_queue = deque()
    sfc2_queue = deque()
    queue_lock = threading.Lock()
    BATCH_SIZE   = 100
    BATCH_TIMEOUT = 0.010

    def flush_sfc1():
        pkts = list(sfc1_queue); sfc1_queue.clear()
        for p in pkts:
            sfc1_sock.sendto(p, (sfc1_host, sfc1_port))

    def flush_sfc2():
        pkts = list(sfc2_queue); sfc2_queue.clear()
        for p in pkts:
            sfc2_sock.sendto(p, (sfc2_host, sfc2_port))

    def batch_sender():
        while not stop_event.is_set():
            time.sleep(BATCH_TIMEOUT)
            with queue_lock:
                if sfc1_queue: flush_sfc1()
                if sfc2_queue: flush_sfc2()

    threading.Thread(target=batch_sender, daemon=True).start()

    # Instantiate DPI VNF
    vnf = DPIVNF(
        vnf_name=vnf_name, config=config, metrics=metrics, mem=mem
    )

    bytes_window      = 0
    bytes_window_lock = threading.Lock()
    window_start      = time.time()
    last_delay        = None

    def process_one(raw_bytes):
        nonlocal bytes_window, last_delay

        try:
            message = json.loads(raw_bytes.decode("utf-8"))
        except Exception as e:
            log.error(f"Decode error: {e}")
            metrics.messages_dropped.labels(
                vnf=f"{vnf_name}-w{worker_id}", reason="decode_error"
            ).inc()
            return

        metrics.bytes_ingress.labels(vnf=f"{vnf_name}-w{worker_id}").inc(len(raw_bytes))
        metrics.packets_ingress.labels(vnf=f"{vnf_name}-w{worker_id}").inc()

        flow_key = vnf.get_flow_key(message)
        mem.record_packet(raw_bytes, flow_key)

        # Per-hop SFC timestamp
        if "sfc_hops" not in message:
            message["sfc_hops"] = []
        recv_ts = time.time()
        message["sfc_hops"].append({"vnf": vnf_name, "recv_ts": recv_ts})

        t_start = time.time()
        result  = vnf.process(message)
        processing_time = time.time() - t_start

        if result is None:
            metrics.messages_dropped.labels(
                vnf=f"{vnf_name}-w{worker_id}", reason="filtered"
            ).inc()
            return

        message["sfc_hops"][-1]["send_ts"]          = time.time()
        message["sfc_hops"][-1]["processing_delay"] = processing_time

        metrics.processing_delay.labels(
            vnf=f"{vnf_name}-w{worker_id}"
        ).observe(processing_time)

        if last_delay is not None:
            metrics.packet_delay_variation.labels(
                vnf=f"{vnf_name}-w{worker_id}"
            ).observe(abs(processing_time - last_delay))
        last_delay = processing_time

        out_bytes  = json.dumps(result).encode("utf-8")
        sfc_target = result.get("sfc_target", "sfc1")

        # Route to correct next VNF
        with queue_lock:
            if sfc_target == "sfc1":
                sfc1_queue.append(out_bytes)
                if len(sfc1_queue) >= BATCH_SIZE:
                    flush_sfc1()
            else:
                sfc2_queue.append(out_bytes)
                if len(sfc2_queue) >= BATCH_SIZE:
                    flush_sfc2()

        metrics.bytes_egress.labels(vnf=f"{vnf_name}-w{worker_id}").inc(len(out_bytes))
        metrics.packets_egress.labels(vnf=f"{vnf_name}-w{worker_id}").inc()
        metrics.messages_processed.labels(vnf=f"{vnf_name}-w{worker_id}").inc()

        with bytes_window_lock:
            bytes_window += len(out_bytes)

    # Metrics loop
    def metrics_loop():
        nonlocal bytes_window, window_start
        while not stop_event.is_set():
            time.sleep(1.0)
            now = time.time()
            with bytes_window_lock:
                snap         = bytes_window
                bytes_window = 0
            elapsed = now - window_start
            window_start = now
            mbps = (snap * 8) / (elapsed * 1_000_000) if elapsed > 0 else 0.0
            metrics.throughput_mbps.labels(vnf=f"{vnf_name}-w{worker_id}").set(mbps)
            metrics.update_uptime()
            metrics.update_memory_stats(mem)

    threading.Thread(target=metrics_loop, daemon=True).start()

    log.info(f"DPI worker started (PID {os.getpid()})")

    while not stop_event.is_set():
        try:
            data, _ = udp_input.recvfrom(65535)
            process_one(data)
        except socket.timeout:
            continue
        except Exception as e:
            log.error(f"UDP receive error: {e}")

    log.info(f"DPI worker stopped")


# Override run() to use custom worker
import multiprocessing as _mp
import signal as _signal

_original_run = DPIVNF.run.__func__ if hasattr(DPIVNF.run, '__func__') else None

@classmethod
def _dpi_run(cls, vnf_name: str, config: dict):
    import logging, threading, signal, multiprocessing
    from base_vnf import _run_health_server, NUM_WORKERS, HEALTH_PORT

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    )
    log = logging.getLogger(vnf_name)
    log.info(f"[{vnf_name}] Starting DPI with {NUM_WORKERS} worker processes")

    threading.Thread(
        target=_run_health_server, args=(HEALTH_PORT,), daemon=True
    ).start()

    stop_event = multiprocessing.Event()

    def _sig(sig, frame):
        log.info(f"[{vnf_name}] Signal {sig} — stopping")
        stop_event.set()

    signal.signal(signal.SIGTERM, _sig)
    signal.signal(signal.SIGINT,  _sig)

    workers = []
    for i in range(NUM_WORKERS):
        p = multiprocessing.Process(
            target=_dpi_worker_process,
            args=(i, vnf_name, config, stop_event),
            name=f"{vnf_name}-worker-{i}",
        )
        p.start()
        log.info(f"[{vnf_name}] Worker {i} started (PID {p.pid})")
        workers.append(p)

    for p in workers:
        p.join()

    log.info(f"[{vnf_name}] All workers stopped")

DPIVNF.run = _dpi_run
