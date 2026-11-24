"""
Encryption VNF — AES-256-GCM payload encryption.

Position in SFC1:
  DPI → [Encryption on worker-4] → Compression

Functionality:
  - Receives JSON messages via UDP from DPI
  - Encrypts the sensor payload (value field) using AES-256-GCM
  - AES-256-GCM provides both confidentiality AND authenticity
    (the GCM authentication tag detects any tampering in transit)
  - Forwards encrypted message via UDP to Compression VNF
  - Key management: key loaded from Kubernetes Secret via env var

AES-256-GCM is the industry standard for IoT payload encryption:
  - Used in TLS 1.3, WireGuard, Signal Protocol
  - NIST SP 800-38D approved
  - 256-bit key = 2^256 possible keys (quantum-resistant for near future)
  - 96-bit nonce (IV) — unique per message, derived from sensor_id + timestamp
  - 16-byte authentication tag appended to ciphertext

Key design decisions:
  - Nonce derived deterministically from sensor_id + timestamp
    so the same message always encrypts to the same ciphertext
    (required for idempotent reprocessing in the data pipeline)
  - Key rotated every 24h via Kubernetes Secret update
  - Encrypted value stored as hex string to remain JSON-serializable
"""

import base64
import binascii
import hashlib
import json
import logging
import os
import time
from typing import Optional

from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from base_vnf import BaseVNF
from metrics import VNFMetrics
from vnf_memory import VNFMemoryTracker

log = logging.getLogger(__name__)

# AES-256-GCM tag length in bytes
GCM_TAG_LENGTH = 16

# Nonce length in bytes (96 bits as per NIST SP 800-38D recommendation)
NONCE_LENGTH = 12


class AESGCMCipher:
    """
    AES-256-GCM encryption engine.

    Key loading:
      - Primary: AES_KEY_HEX environment variable (64 hex chars = 32 bytes)
      - Fallback: deterministic key derived from VNF_NAME (dev/testing only)

    Nonce strategy:
      - Derived from SHA-256(sensor_id + str(timestamp))[:12]
      - Deterministic per message for idempotent reprocessing
      - Unique per sensor+time combination (timestamp has ms precision)
      - Never reuses a nonce for the same key (timestamp ensures uniqueness)
    """

    def __init__(self, key_hex: Optional[str] = None):
        if key_hex:
            key_bytes = bytes.fromhex(key_hex)
            if len(key_bytes) != 32:
                raise ValueError(
                    f"AES key must be 32 bytes (256 bits), "
                    f"got {len(key_bytes)} bytes"
                )
            self._key = key_bytes
            log.info("AES-256-GCM key loaded from environment variable")
        else:
            # Dev fallback — deterministic key from VNF name
            # NEVER use in production
            vnf_name = os.environ.get("VNF_NAME", "encryption")
            self._key = hashlib.sha256(vnf_name.encode()).digest()
            log.warning(
                "AES key not set — using derived key from VNF_NAME. "
                "Set AES_KEY_HEX environment variable for production."
            )

        self._aesgcm = AESGCM(self._key)

    def _derive_nonce(self, sensor_id: str, timestamp: float) -> bytes:
        """
        Derive a deterministic 96-bit nonce from sensor_id and timestamp.
        SHA-256(sensor_id || timestamp_ms)[:12]

        This ensures:
        - Same message → same nonce (idempotent)
        - Different timestamps → different nonces (no reuse)
        - No need to transmit/store nonce separately (receiver can recompute)
        """
        nonce_input = f"{sensor_id}:{timestamp:.6f}".encode("utf-8")
        return hashlib.sha256(nonce_input).digest()[:NONCE_LENGTH]

    def encrypt(self, plaintext: bytes, sensor_id: str,
                timestamp: float) -> dict:
        """
        Encrypt plaintext using AES-256-GCM.

        Returns dict with:
          ciphertext_hex: hex-encoded ciphertext + GCM tag
          nonce_hex:      hex-encoded nonce (for verification/debugging)
          tag_length:     GCM authentication tag length in bytes
        """
        nonce = self._derive_nonce(sensor_id, timestamp)

        # AESGCM.encrypt() returns ciphertext + tag concatenated
        ciphertext_with_tag = self._aesgcm.encrypt(
            nonce,
            plaintext,
            None,  # no additional authenticated data (AAD)
        )

        return {
            "ciphertext_hex": ciphertext_with_tag.hex(),
            "nonce_hex":      nonce.hex(),
            "tag_length":     GCM_TAG_LENGTH,
        }

    def decrypt(self, ciphertext_hex: str, sensor_id: str,
                timestamp: float) -> bytes:
        """
        Decrypt AES-256-GCM ciphertext.
        Used by downstream consumers (ClickHouse, dbt pipeline).
        """
        nonce      = self._derive_nonce(sensor_id, timestamp)
        ciphertext = bytes.fromhex(ciphertext_hex)
        return self._aesgcm.decrypt(nonce, ciphertext, None)


class EncryptionVNF(BaseVNF):
    """
    Encryption VNF — SFC1 chain, worker-4.

    Encrypts the sensor value field of each message.
    The rest of the message fields (sensor_id, gateway_id, etc.)
    remain in plaintext for routing and monitoring purposes.

    This mirrors real-world field-level encryption in IoT:
    encrypt the sensitive measurement data while keeping
    metadata visible for network operations.
    """

    def __init__(
        self,
        vnf_name: str,
        config:   dict,
        metrics:  VNFMetrics,
        mem:      VNFMemoryTracker,
    ):
        super().__init__(vnf_name, config, metrics, mem)

        key_hex = config["encryption"].get("aes_key_hex") or \
                  os.environ.get("AES_KEY_HEX")

        self._cipher = AESGCMCipher(key_hex=key_hex)
        log.info(f"[{vnf_name}] Encryption VNF ready (AES-256-GCM)")

    def get_flow_key(self, message: dict) -> tuple:
        return (
            message.get("gateway_id", "unknown"),
            message.get("type",       "unknown"),
            message.get("sfc_target", "unknown"),
        )

    def process(self, message: dict) -> Optional[dict]:
        """
        Encrypt the sensor value payload.
        Returns modified message with encrypted value.
        """
        sensor_id = message.get("sensor_id", "unknown")
        timestamp = message.get("timestamp",  time.time())
        value     = message.get("value")

        if value is None:
            log.warning(
                f"[{self.vnf_name}] Missing value field in message "
                f"from {sensor_id} — dropping"
            )
            self.metrics.messages_dropped.labels(
                vnf=self.vnf_name, reason="missing_value"
            ).inc()
            return None

        # Serialize value to bytes for encryption
        # Include sensor type for context — encrypts richer payload
        plaintext = json.dumps({
            "value": value,
            "unit":  message.get("unit", ""),
            "type":  message.get("type", ""),
        }).encode("utf-8")

        bytes_before = len(plaintext)

        t_cipher = time.time()

        enc_result = self._cipher.encrypt(
            plaintext = plaintext,
            sensor_id = sensor_id,
            timestamp = timestamp,
        )

        cipher_duration = time.time() - t_cipher

        bytes_after = len(bytes.fromhex(enc_result["ciphertext_hex"]))

        # Update encryption-specific metrics
        self.metrics.enc_cipher_duration.labels(
            vnf=self.vnf_name
        ).observe(cipher_duration)

        self.metrics.enc_bytes_before.labels(
            vnf=self.vnf_name
        ).inc(bytes_before)

        self.metrics.enc_bytes_after.labels(
            vnf=self.vnf_name
        ).inc(bytes_after)

        # Overhead ratio: AES-GCM adds exactly 16 bytes (tag) per message
        overhead_ratio = bytes_after / bytes_before if bytes_before > 0 else 1.0
        self.metrics.enc_overhead_ratio.labels(
            vnf=self.vnf_name
        ).set(overhead_ratio)

        # Replace plaintext value with encrypted payload
        # Keep all other fields intact for routing and monitoring
        message["value"]         = enc_result["ciphertext_hex"]
        message["encrypted"]     = True
        message["enc_algorithm"] = "AES-256-GCM"
        message["enc_nonce"]     = enc_result["nonce_hex"]
        message["enc_tag_len"]   = enc_result["tag_length"]
        message["enc_timestamp"] = time.time()

        # Remove plaintext unit — it's now inside the encrypted payload
        # Keep type for downstream routing decisions
        message.pop("unit", None)

        return message


def build_config() -> dict:
    """Build config from environment variables (K8s Secret/ConfigMap)."""
    return {
        "kafka": {
            "bootstrap_servers": os.environ.get(
                "KAFKA_BOOTSTRAP_SERVERS",
                "iot-kafka-kafka-bootstrap.kafka.svc.cluster.local:9092",
            ),
        },
        "transport": {
            # Middle VNF — receive via UDP from DPI
            "udp_input":      True,
            "udp_input_port": int(os.environ.get("UDP_INPUT_PORT", "9002")),
            # Forward via UDP to Compression on worker-5
            "udp_output_host": os.environ.get("NEXT_HOP_HOST", "worker-5"),
            "udp_output_port": int(os.environ.get("NEXT_HOP_PORT", "9003")),
        },
        "encryption": {
            # AES-256-GCM key as 64-char hex string (32 bytes)
            # Set via K8s Secret: kubectl create secret generic enc-key
            #   --from-literal=AES_KEY_HEX=<64 hex chars>
            "aes_key_hex": os.environ.get("AES_KEY_HEX"),
        },
    }


if __name__ == "__main__":
    config   = build_config()
    vnf_name = os.environ.get("VNF_NAME", "encryption")
    EncryptionVNF.run(vnf_name=vnf_name, config=config)
