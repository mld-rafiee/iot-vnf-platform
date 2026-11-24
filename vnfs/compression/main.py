"""
Compression VNF — Brotli payload compression.

Position in SFC1 (last VNF):
  Encryption → [Compression on worker-5] → Kafka(sfc1-egress)

Functionality:
  - Receives JSON messages via UDP from Encryption VNF
  - Compresses the encrypted payload using Brotli
  - Base64-encodes the compressed bytes for JSON serialization
  - Produces the final processed message to Kafka sfc1-egress topic

Why Brotli:
  - Developed by Google, designed for network data compression
  - 20-26% better compression ratio than gzip/zlib on JSON-like data
  - Widely used in production: nginx, Cloudflare, Fastly
  - Quality levels 0-11: 0=fastest, 11=best compression
  - Quality 4 is the production sweet spot: good ratio, low CPU
  - Brotli compresses JSON particularly well due to repetitive
    field names and structure

Compression quality selection:
  Quality 0-3:  Real-time streaming (sub-millisecond)
  Quality 4-6:  Production IoT data pipelines (our choice)
  Quality 7-9:  Batch processing, archival
  Quality 10-11: Maximum compression, offline only

References:
  - RFC 7932: Brotli Compressed Data Format
  - Google Brotli: https://github.com/google/brotli
  - Your existing code: compression-vnf_stateful.py (brotli + base64)
"""

import base64
import json
import logging
import os
import time
from typing import Optional

import brotli

from base_vnf import BaseVNF
from metrics import VNFMetrics
from vnf_memory import VNFMemoryTracker

log = logging.getLogger(__name__)

# Brotli quality level — 4 is production sweet spot
# Quality 4: ~2-5ms compression time, ~40-60% size reduction on JSON
DEFAULT_QUALITY = int(os.environ.get("BROTLI_QUALITY", "4"))

# Brotli window size (log2 of window size in bytes)
# 22 = 4MB window — good for typical IoT message sizes
DEFAULT_LGWIN = int(os.environ.get("BROTLI_LGWIN", "22"))


class BrotliCompressor:
    """
    Brotli compression engine.

    Compresses data and base64-encodes the result for safe
    JSON serialization — same approach as your existing code.

    Tracks compression ratio statistics for metrics.
    """

    def __init__(self, quality: int = DEFAULT_QUALITY,
                 lgwin: int = DEFAULT_LGWIN):
        self._quality = quality
        self._lgwin   = lgwin
        log.info(
            f"Brotli compressor initialized "
            f"(quality={quality}, lgwin={lgwin})"
        )

    def compress(self, data: bytes) -> dict:
        """
        Compress bytes using Brotli and base64-encode the result.

        Returns dict:
          compressed_b64: base64-encoded compressed bytes
          original_size:  original size in bytes
          compressed_size: compressed size in bytes (before base64)
          ratio:          compressed_size / original_size (lower = better)
        """
        compressed = brotli.compress(
            data,
            quality = self._quality,
            lgwin   = self._lgwin,
        )
        compressed_b64 = base64.b64encode(compressed).decode("ascii")

        original_size   = len(data)
        compressed_size = len(compressed)
        ratio = compressed_size / original_size if original_size > 0 else 1.0

        return {
            "compressed_b64":  compressed_b64,
            "original_size":   original_size,
            "compressed_size": compressed_size,
            "ratio":           ratio,
        }

    def decompress(self, compressed_b64: str) -> bytes:
        """
        Decompress base64-encoded Brotli data.
        Used by downstream consumers (MinIO → ClickHouse pipeline).
        """
        compressed = base64.b64decode(compressed_b64.encode("ascii"))
        return brotli.decompress(compressed)


class CompressionVNF(BaseVNF):
    """
    Compression VNF — SFC1 chain, worker-5.
    Last VNF in SFC1 — produces to Kafka sfc1-egress.

    Compresses the encrypted payload field.
    Since the payload is already AES-GCM encrypted (high entropy),
    compression after encryption yields minimal benefit for the
    encrypted value field. Therefore we compress the full message
    JSON (which includes plaintext metadata with repetitive structure)
    rather than just the encrypted value.

    This matches real-world practice: compress → encrypt gives better
    ratio than encrypt → compress, but our SFC order is defined by
    the paper's topology (ENC before COMP). We work within that
    constraint by compressing the full message envelope.
    """

    def __init__(
        self,
        vnf_name: str,
        config:   dict,
        metrics:  VNFMetrics,
        mem:      VNFMemoryTracker,
    ):
        super().__init__(vnf_name, config, metrics, mem)

        quality = config["compression"].get("brotli_quality", DEFAULT_QUALITY)
        lgwin   = config["compression"].get("brotli_lgwin",   DEFAULT_LGWIN)

        self._compressor = BrotliCompressor(quality=quality, lgwin=lgwin)
        log.info(f"[{vnf_name}] Compression VNF ready (Brotli q={quality})")

    def get_flow_key(self, message: dict) -> tuple:
        return (
            message.get("gateway_id", "unknown"),
            message.get("type",       "unknown"),
            message.get("sfc_target", "unknown"),
        )

    def process(self, message: dict) -> Optional[dict]:
        """
        Compress the full message payload.
        Returns a wrapper message with the compressed original inside.
        """
        # Serialize the full incoming message to bytes
        message_bytes = json.dumps(message).encode("utf-8")
        bytes_before  = len(message_bytes)

        t_comp = time.time()

        result = self._compressor.compress(message_bytes)

        comp_duration = time.time() - t_comp
        bytes_after   = result["compressed_size"]
        ratio         = result["ratio"]

        # Update compression metrics
        self.metrics.comp_duration.labels(
            vnf=self.vnf_name
        ).observe(comp_duration)

        self.metrics.comp_bytes_before.labels(
            vnf=self.vnf_name
        ).inc(bytes_before)

        self.metrics.comp_bytes_after.labels(
            vnf=self.vnf_name
        ).inc(bytes_after)

        self.metrics.comp_ratio.labels(
            vnf=self.vnf_name
        ).observe(ratio)

        log.debug(
            f"[{self.vnf_name}] Compressed "
            f"{bytes_before}B → {bytes_after}B "
            f"(ratio={ratio:.3f}, {comp_duration*1000:.2f}ms)"
        )

        # Build final egress message
        # The compressed payload is the original message in compressed form
        # Downstream consumers decompress → decrypt → store in ClickHouse
        egress_message = {
            # Routing and identification — kept plaintext for pipeline routing
            "sensor_id":        message.get("sensor_id"),
            "gateway_id":       message.get("gateway_id"),
            "sfc_target":       message.get("sfc_target"),
            "original_type":    message.get("type"),
            "original_ts":      message.get("timestamp"),

            # Compressed encrypted payload
            "payload_compressed": result["compressed_b64"],
            "compression":        "brotli",
            "compression_quality": self._compressor._quality,
            "original_size":      bytes_before,
            "compressed_size":    bytes_after,
            "compression_ratio":  ratio,

            # SFC chain metadata
            "sfc_hops":          message.get("sfc_hops", []),
            "fw_passed":         message.get("fw_passed", False),
            "dpi_clean":         message.get("dpi_clean", True),
            "encrypted":         message.get("encrypted", False),
            "enc_algorithm":     message.get("enc_algorithm"),

            # Egress timestamp
            "comp_timestamp":    time.time(),
            "egress_ts":         time.time(),
        }

        return egress_message


def build_config() -> dict:
    return {
        "kafka": {
            "bootstrap_servers": os.environ.get(
                "KAFKA_BOOTSTRAP_SERVERS",
                "iot-kafka-kafka-bootstrap.kafka.svc.cluster.local:9092",
            ),
        },
        "transport": {
            # Middle/exit VNF — receive via UDP from Encryption
            "udp_input":      True,
            "udp_input_port": int(os.environ.get("UDP_INPUT_PORT", "9003")),
            # Exit VNF — produce to Kafka sfc1-egress
            "kafka_output":       True,
            "kafka_output_topic": os.environ.get(
                "OUTPUT_TOPIC", "sfc1-egress"
            ),
        },
        "compression": {
            "brotli_quality": int(os.environ.get("BROTLI_QUALITY", "4")),
            "brotli_lgwin":   int(os.environ.get("BROTLI_LGWIN",   "22")),
        },
    }


if __name__ == "__main__":
    config   = build_config()
    vnf_name = os.environ.get("VNF_NAME", "compression")
    CompressionVNF.run(vnf_name=vnf_name, config=config)
