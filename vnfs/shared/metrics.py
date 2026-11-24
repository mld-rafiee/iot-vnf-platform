"""
VNF Prometheus Metrics
======================
Shared metrics definitions for all VNFs.

References:
- ETSI GS NFV-IFA 027 V5.4.1 (2026-02): Performance Measurements Specification
- ETSI GS NFV-INF 010: Service Quality Metrics
- ETSI GS NFV-TST 001: Pre-deployment Testing
- OPNFV Yardstick KPIs
- RFC 3550: RTP jitter definition
- PPTMon (Van Tu et al., IEEE TNSM 2021): per-hop VNF processing time

Usage:
    from metrics import VNFMetrics
    m = VNFMetrics(vnf_name="firewall-1")
    m.start_server()
    m.messages_processed.labels(vnf="firewall-1").inc()
    m.processing_delay.labels(vnf="firewall-1").observe(0.002)
"""

import time
import logging

from prometheus_client import (
    Counter,
    Gauge,
    Histogram,
    start_http_server,
)

log = logging.getLogger(__name__)

# ── Histogram buckets ──────────────────────────────────────────────────────────

# Latency buckets in seconds: 10us → 10s
# Covers both fast VNFs (ENC ~100us) and slow ones (DPI with Snort ~10ms)
LATENCY_BUCKETS = (
    0.00001, 0.00005,
    0.0001,  0.0005,
    0.001,   0.005,
    0.01,    0.05,
    0.1,     0.5,
    1.0,     5.0,   10.0,
)

# Compression ratio buckets: 0.0 (perfect) to 1.0 (no compression)
RATIO_BUCKETS = (
    0.1, 0.2, 0.3, 0.4, 0.5,
    0.6, 0.7, 0.8, 0.9, 1.0,
)


class VNFMetrics:
    """
    Prometheus metrics for one VNF worker process.
    One instance per worker process — no sharing across processes.

    All metrics are labelled with vnf= for multi-VNF Grafana dashboards.
    Label value includes worker ID: e.g. "firewall-1-w0", "firewall-1-w1"
    """

    def __init__(self, vnf_name: str, port: int = 8000):
        self.vnf_name   = vnf_name
        self._port      = port
        self._start_time = time.time()

        # ── ETSI IFA-027: VNF Connection Point metrics ─────────────────────
        # ByteIncomingVnfIntCp — bytes received at VNF ingress CP
        self.bytes_ingress = Counter(
            "vnf_bytes_ingress_total",
            "Total bytes received at VNF ingress connection point "
            "(ETSI IFA-027 ByteIncomingVnfIntCp)",
            ["vnf"],
        )

        # ByteOutgoingVnfIntCp — bytes sent at VNF egress CP
        self.bytes_egress = Counter(
            "vnf_bytes_egress_total",
            "Total bytes sent at VNF egress connection point "
            "(ETSI IFA-027 ByteOutgoingVnfIntCp)",
            ["vnf"],
        )

        # PacketIncomingVnfIntCp — packets received at VNF ingress CP
        self.packets_ingress = Counter(
            "vnf_packets_ingress_total",
            "Total packets received at VNF ingress connection point "
            "(ETSI IFA-027 PacketIncomingVnfIntCp)",
            ["vnf"],
        )

        # PacketOutgoingVnfIntCp — packets sent at VNF egress CP
        self.packets_egress = Counter(
            "vnf_packets_egress_total",
            "Total packets sent at VNF egress connection point "
            "(ETSI IFA-027 PacketOutgoingVnfIntCp)",
            ["vnf"],
        )

        # ── ETSI INF-010 + OPNFV Yardstick: core network QoS metrics ───────

        # Processing delay histogram (p50, p95, p99)
        # From paper + ETSI TST009 + PPTMon per-hop measurement
        self.processing_delay = Histogram(
            "vnf_processing_delay_seconds",
            "Time from message received to message forwarded at this VNF hop. "
            "Enables end-to-end SFC chain delay per ETSI TST001 and PPTMon.",
            ["vnf"],
            buckets=LATENCY_BUCKETS,
        )

        # Packet Delay Variation / Jitter (RFC 3550, ETSI INF-010)
        # jitter = |D(n) - D(n-1)| where D(n) is processing delay of packet n
        # Critical for real-time IoT applications with latency SLAs
        self.packet_delay_variation = Histogram(
            "vnf_packet_delay_variation_seconds",
            "Packet delay variation (jitter) per RFC 3550: |D(n) - D(n-1)|. "
            "Required by ETSI INF-010 for VNF service quality monitoring.",
            ["vnf"],
            buckets=LATENCY_BUCKETS,
        )

        # Messages successfully processed
        self.messages_processed = Counter(
            "vnf_messages_processed_total",
            "Total messages successfully processed and forwarded",
            ["vnf"],
        )

        # Messages dropped — labelled by reason for root cause analysis
        self.messages_dropped = Counter(
            "vnf_messages_dropped_total",
            "Total messages dropped. "
            "reason: decode_error | queue_full | filtered | processing_error",
            ["vnf", "reason"],
        )

        # Packet loss ratio gauge (ETSI INF-010)
        # Computed as: dropped / (processed + dropped) over sliding window
        self.packet_loss_ratio = Gauge(
            "vnf_packet_loss_ratio",
            "Ratio of dropped to total messages (ETSI INF-010). "
            "Updated every reporting interval.",
            ["vnf"],
        )

        # Current throughput in Mbps (rolling 1s window)
        self.throughput_mbps = Gauge(
            "vnf_throughput_mbps",
            "Current throughput in Mbps (rolling 1-second window). "
            "From paper KPI: traffic_rate_mbps.",
            ["vnf"],
        )

        # Processing queue depth
        self.queue_depth = Gauge(
            "vnf_queue_depth",
            "Current internal processing queue depth",
            ["vnf"],
        )

        # Active flows in connection/state table (from VNFMemoryTracker)
        self.active_flows = Gauge(
            "vnf_active_flows",
            "Number of active flows in connection tracking table",
            ["vnf"],
        )

        # Memory used by sliding window packet log (from VNFMemoryTracker)
        self.packet_log_bytes = Gauge(
            "vnf_packet_log_bytes",
            "Memory used by sliding window packet log in bytes",
            ["vnf"],
        )

        # CPU utilization self-reported by VNF process (ETSI TST001)
        # Uses resource.getrusage() — reports this process only
        # Separate from K8s node-exporter which reports the whole node
        self.cpu_utilization = Gauge(
            "vnf_cpu_utilization_percent",
            "VNF process CPU utilization percentage (self-reported via "
            "resource.getrusage). Separate from node-level metrics. "
            "ETSI TST001 Processor Utilization.",
            ["vnf"],
        )

        # Uptime in seconds
        self.uptime = Gauge(
            "vnf_uptime_seconds",
            "Seconds since this VNF worker process started",
            ["vnf"],
        )

        # Network outage counter (ETSI INF-010)
        # Incremented when VNF fails to forward a message due to network error
        self.network_outage = Counter(
            "vnf_network_outage_total",
            "Number of times VNF failed to forward due to network error "
            "(ETSI INF-010 Network Outage)",
            ["vnf"],
        )

        # ── Firewall-specific metrics ───────────────────────────────────────

        # Messages matched by ALLOW rules
        self.fw_rules_matched = Counter(
            "vnf_fw_rules_matched_total",
            "Messages matched and allowed by firewall ruleset",
            ["vnf"],
        )

        # Messages blocked by DENY rules
        self.fw_rules_blocked = Counter(
            "vnf_fw_rules_blocked_total",
            "Messages blocked by firewall ruleset",
            ["vnf"],
        )

        # Rule lookup duration — how long nftables evaluation takes
        self.fw_rule_lookup_duration = Histogram(
            "vnf_fw_rule_lookup_duration_seconds",
            "Time to evaluate nftables rules for one message",
            ["vnf"],
            buckets=LATENCY_BUCKETS,
        )

        # Stateful connection table size
        self.fw_active_connections = Gauge(
            "vnf_fw_active_connections",
            "Number of active stateful connections in firewall table",
            ["vnf"],
        )

        # SYN flood protection triggers
        self.fw_syn_flood_detected = Counter(
            "vnf_fw_syn_flood_detected_total",
            "Number of SYN flood protection triggers",
            ["vnf"],
        )

        # ── DPI-specific metrics ────────────────────────────────────────────

        # Snort3 alerts — labelled by rule category
        # categories: malware, policy, protocol, exploit, dos, recon
        self.dpi_alerts = Counter(
            "vnf_dpi_alerts_total",
            "Snort3 IDS alerts triggered, labelled by rule category",
            ["vnf", "category"],
        )

        # Messages passing DPI clean
        self.dpi_clean = Counter(
            "vnf_dpi_clean_total",
            "Messages passing DPI inspection without alert",
            ["vnf"],
        )

        # Snort3 inspection duration
        self.dpi_inspection_duration = Histogram(
            "vnf_dpi_inspection_duration_seconds",
            "Time for Snort3 to inspect one message payload",
            ["vnf"],
            buckets=LATENCY_BUCKETS,
        )

        # Routing decisions
        self.dpi_sfc1_routed = Counter(
            "vnf_dpi_sfc1_routed_total",
            "Messages routed to SFC1 chain (ENC path)",
            ["vnf"],
        )

        self.dpi_sfc2_routed = Counter(
            "vnf_dpi_sfc2_routed_total",
            "Messages routed to SFC2 chain (NAT path)",
            ["vnf"],
        )

        # ── Encryption-specific metrics ─────────────────────────────────────

        # AES-GCM cipher duration
        self.enc_cipher_duration = Histogram(
            "vnf_enc_cipher_duration_seconds",
            "Time to AES-GCM encrypt one message payload",
            ["vnf"],
            buckets=LATENCY_BUCKETS,
        )

        # Bytes before and after encryption — measures GCM tag overhead
        self.enc_bytes_before = Counter(
            "vnf_enc_bytes_before_total",
            "Total payload bytes before AES-GCM encryption",
            ["vnf"],
        )

        self.enc_bytes_after = Counter(
            "vnf_enc_bytes_after_total",
            "Total payload bytes after AES-GCM encryption "
            "(includes 16-byte GCM authentication tag overhead per message)",
            ["vnf"],
        )

        # Encryption overhead ratio gauge
        self.enc_overhead_ratio = Gauge(
            "vnf_enc_overhead_ratio",
            "Current encryption size overhead ratio (after/before). "
            "AES-GCM adds 16-byte tag per message so ratio > 1.0.",
            ["vnf"],
        )

        # ── Compression-specific metrics ────────────────────────────────────

        # Brotli compression duration
        self.comp_duration = Histogram(
            "vnf_comp_duration_seconds",
            "Time to Brotli compress one message payload",
            ["vnf"],
            buckets=LATENCY_BUCKETS,
        )

        # Bytes before and after compression
        self.comp_bytes_before = Counter(
            "vnf_comp_bytes_before_total",
            "Total payload bytes before Brotli compression",
            ["vnf"],
        )

        self.comp_bytes_after = Counter(
            "vnf_comp_bytes_after_total",
            "Total payload bytes after Brotli compression",
            ["vnf"],
        )

        # Compression ratio distribution
        self.comp_ratio = Histogram(
            "vnf_comp_ratio",
            "Compression ratio distribution (after/before). "
            "Lower is better. Brotli typically achieves 0.3-0.5 on JSON.",
            ["vnf"],
            buckets=RATIO_BUCKETS,
        )

        # ── NAT-specific metrics ────────────────────────────────────────────

        # Active NAT table entries
        self.nat_translations_active = Gauge(
            "vnf_nat_translations_active",
            "Number of active entries in NAT translation table",
            ["vnf"],
        )

        # Port pool usage
        self.nat_port_pool_used = Gauge(
            "vnf_nat_port_pool_used",
            "Number of ports currently allocated from NAT port pool",
            ["vnf"],
        )

        # Port pool exhaustion events
        self.nat_port_pool_exhausted = Counter(
            "vnf_nat_port_pool_exhausted_total",
            "Number of times NAT port pool wrapped around (pool exhaustion)",
            ["vnf"],
        )

        # NAT translation duration
        self.nat_translation_duration = Histogram(
            "vnf_nat_translation_duration_seconds",
            "Time to perform source NAT rewrite on one message",
            ["vnf"],
            buckets=LATENCY_BUCKETS,
        )

    # ── Server and helpers ──────────────────────────────────────────────────────

    def start_server(self):
        """Start Prometheus HTTP metrics server."""
        start_http_server(self._port)
        log.info(
            f"[{self.vnf_name}] Prometheus metrics at "
            f"http://0.0.0.0:{self._port}/metrics"
        )

    def update_uptime(self):
        """Update uptime gauge. Call from background thread."""
        self.uptime.labels(vnf=self.vnf_name).set(
            time.time() - self._start_time
        )

    def update_memory_stats(self, mem_tracker):
        """
        Pull stats from VNFMemoryTracker and update gauges.
        Call from background thread.
        """
        stats = mem_tracker.stats()
        self.active_flows.labels(vnf=self.vnf_name).set(
            stats["active_flows"]
        )
        self.packet_log_bytes.labels(vnf=self.vnf_name).set(
            stats["packet_log_bytes"]
        )

    def update_packet_loss_ratio(self, processed: int, dropped: int):
        """
        Compute and update packet loss ratio.
        Call from background thread with sliding window counts.
        ETSI INF-010: packet_loss_ratio = dropped / (processed + dropped)
        """
        total = processed + dropped
        ratio = dropped / total if total > 0 else 0.0
        self.packet_loss_ratio.labels(vnf=self.vnf_name).set(ratio)