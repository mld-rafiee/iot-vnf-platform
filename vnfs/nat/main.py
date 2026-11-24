"""
NAT VNF — Network Address Translation.

Position in SFC2 (last VNF):
  DPI → [NAT on worker-6] → Kafka(sfc2-egress)

Functionality:
  - Receives JSON messages via UDP from DPI
  - Performs Source NAT (SNAT): rewrites the gateway_id and
    assigns a translated port from a port pool
  - Maintains a stateful NAT translation table (same design as
    your existing nat-vnf_stateful.py)
  - Produces the NAT-translated message to Kafka sfc2-egress

Why NAT for IoT:
  In real deployments, IoT gateways sit behind NAT — their
  private addresses are rewritten to a public routable address
  before traffic reaches the core network. This VNF simulates
  that boundary: gateway IDs (analogous to private IPs) are
  mapped to translated IDs (analogous to public IP:port).

  This is exactly the architecture in your paper:
  SFC2 handles network sensor data routed through a NAT chain
  before forwarding to the data pipeline.

NAT table design:
  - Port pool: 10000-60000 (50,000 ports — same as your code)
  - Translation key: (gateway_id, sensor_type) → nat_port
  - Reverse table: nat_port → (original_gateway_id, sensor_type)
  - Thread-safe with per-process lock (each worker has own table)
  - No expiry: ports held for session lifetime (stateful NAT)

References:
  - RFC 2663: IP Network Address Translator Terminology
  - RFC 3022: Traditional IP NAT
  - Your existing code: nat-vnf_stateful.py
"""

import json
import logging
import os
import threading
import time
from typing import Optional

from base_vnf import BaseVNF
from metrics import VNFMetrics
from vnf_memory import VNFMemoryTracker

log = logging.getLogger(__name__)

# NAT port pool range (matches your existing code)
PORT_POOL_START = 10000
PORT_POOL_END   = 60000

# Public NAT address — analogous to the public IP after SNAT
# In the testbed this is a logical identifier, not a real IP
NAT_PUBLIC_ID = os.environ.get("NAT_PUBLIC_ID", "nat-gateway-public")


class NATTable:
    """
    Stateful NAT translation table.

    Maintains bidirectional mapping:
      Forward:  (gateway_id, sensor_type) → nat_port
      Reverse:  nat_port → (gateway_id, sensor_type)

    Port allocation is sequential with wraparound — same as your
    existing code. Thread-safe within one worker process.
    """

    def __init__(
        self,
        pool_start: int = PORT_POOL_START,
        pool_end:   int = PORT_POOL_END,
    ):
        self._pool_start  = pool_start
        self._pool_end    = pool_end
        self._next_port   = pool_start

        # Forward table: translation key → nat_port
        self._forward: dict = {}
        # Reverse table: nat_port → (gateway_id, sensor_type)
        self._reverse: dict = {}

        self._lock        = threading.Lock()
        self._wraparounds = 0

        log.info(
            f"NAT table initialized "
            f"(pool: {pool_start}-{pool_end}, "
            f"{pool_end - pool_start} ports available)"
        )

    def translate(self, gateway_id: str, sensor_type: str) -> dict:
        """
        Perform SNAT translation.

        Returns translation result:
          nat_port:         allocated port from pool
          original_gateway: original gateway_id (pre-NAT)
          nat_gateway:      translated gateway ID (post-NAT)
          is_new:           True if new translation, False if existing
        """
        key = (gateway_id, sensor_type)

        with self._lock:
            if key in self._forward:
                # Existing translation — reuse allocated port
                nat_port = self._forward[key]
                return {
                    "nat_port":         nat_port,
                    "original_gateway": gateway_id,
                    "nat_gateway":      f"{NAT_PUBLIC_ID}:{nat_port}",
                    "is_new":           False,
                }

            # New translation — allocate next port from pool
            nat_port          = self._next_port
            self._next_port  += 1

            if self._next_port > self._pool_end:
                self._next_port   = self._pool_start
                self._wraparounds += 1
                log.warning(
                    f"NAT port pool exhausted — wrapping around "
                    f"(wraparound #{self._wraparounds})"
                )

            self._forward[key]      = nat_port
            self._reverse[nat_port] = (gateway_id, sensor_type)

            return {
                "nat_port":         nat_port,
                "original_gateway": gateway_id,
                "nat_gateway":      f"{NAT_PUBLIC_ID}:{nat_port}",
                "is_new":           True,
            }

    def reverse_lookup(self, nat_port: int) -> Optional[tuple]:
        """
        Look up original (gateway_id, sensor_type) from nat_port.
        Used for return traffic (inbound to IoT network).
        """
        with self._lock:
            return self._reverse.get(nat_port)

    def stats(self) -> dict:
        with self._lock:
            return {
                "active_translations": len(self._forward),
                "ports_used":          len(self._forward),
                "ports_available":     self._pool_end - self._pool_start,
                "next_port":           self._next_port,
                "wraparounds":         self._wraparounds,
            }


class NATVNF(BaseVNF):
    """
    NAT VNF — SFC2 chain, worker-6.
    Last VNF in SFC2 — produces to Kafka sfc2-egress.

    Performs Source NAT on each message:
      - Rewrites gateway_id to NAT public ID + allocated port
      - Preserves original gateway_id for reverse lookup
      - Updates NAT translation table
    """

    def __init__(
        self,
        vnf_name: str,
        config:   dict,
        metrics:  VNFMetrics,
        mem:      VNFMemoryTracker,
    ):
        super().__init__(vnf_name, config, metrics, mem)

        self._nat = NATTable(
            pool_start = config["nat"].get("pool_start", PORT_POOL_START),
            pool_end   = config["nat"].get("pool_end",   PORT_POOL_END),
        )

        log.info(f"[{vnf_name}] NAT VNF ready")

    def get_flow_key(self, message: dict) -> tuple:
        return (
            message.get("gateway_id", "unknown"),
            message.get("type",       "unknown"),
            message.get("sfc_target", "unknown"),
        )

    def process(self, message: dict) -> Optional[dict]:
        """
        Perform SNAT translation on the message.
        Rewrites gateway_id to NAT public address.
        """
        gateway_id  = message.get("gateway_id", "unknown")
        sensor_type = message.get("type",        "unknown")

        t_nat = time.time()

        # Perform NAT translation
        translation = self._nat.translate(
            gateway_id  = gateway_id,
            sensor_type = sensor_type,
        )

        nat_duration = time.time() - t_nat

        # Update NAT-specific metrics
        self.metrics.nat_translation_duration.labels(
            vnf=self.vnf_name
        ).observe(nat_duration)

        nat_stats = self._nat.stats()

        self.metrics.nat_translations_active.labels(
            vnf=self.vnf_name
        ).set(nat_stats["active_translations"])

        self.metrics.nat_port_pool_used.labels(
            vnf=self.vnf_name
        ).set(nat_stats["ports_used"])

        if translation["is_new"] and nat_stats["wraparounds"] > 0:
            self.metrics.nat_port_pool_exhausted.labels(
                vnf=self.vnf_name
            ).inc()

        # Apply NAT translation to message
        # Rewrite gateway_id — analogous to rewriting source IP in SNAT
        message["original_gateway_id"] = gateway_id
        message["gateway_id"]          = translation["nat_gateway"]
        message["nat_port"]            = translation["nat_port"]
        message["nat_public_id"]       = NAT_PUBLIC_ID
        message["nat_timestamp"]       = time.time()
        message["egress_ts"]           = time.time()

        # Add SFC chain completion metadata
        message["sfc2_complete"] = True

        return message


def build_config() -> dict:
    return {
        "kafka": {
            "bootstrap_servers": os.environ.get(
                "KAFKA_BOOTSTRAP_SERVERS",
                "iot-kafka-kafka-bootstrap.kafka.svc.cluster.local:9092",
            ),
        },
        "transport": {
            # Middle/exit VNF — receive via UDP from DPI
            "udp_input":      True,
            "udp_input_port": int(os.environ.get("UDP_INPUT_PORT", "9004")),
            # Exit VNF — produce to Kafka sfc2-egress
            "kafka_output":       True,
            "kafka_output_topic": os.environ.get(
                "OUTPUT_TOPIC", "sfc2-egress"
            ),
        },
        "nat": {
            "pool_start": int(os.environ.get("NAT_POOL_START", str(PORT_POOL_START))),
            "pool_end":   int(os.environ.get("NAT_POOL_END",   str(PORT_POOL_END))),
        },
    }


if __name__ == "__main__":
    config   = build_config()
    vnf_name = os.environ.get("VNF_NAME", "nat")
    NATVNF.run(vnf_name=vnf_name, config=config)
