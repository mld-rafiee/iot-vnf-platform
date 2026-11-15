"""
NNE (NorNet Edge) bandwidth shaper.

Reads real 4G/5G upload speed measurements from node_950_C.csv
and provides the current transmission rate to the gateway.

From your paper:
  "Upload speed measurements from the NorthNetEdge (NNE) dataset,
   collected across Norway between November 2023 and February 2024
   from operational 4G and 5G networks, were used to shape the
   volume and timing of data transmitted from the IoT layer."
"""
import csv
import logging
import time

log = logging.getLogger(__name__)


class NNEBandwidthShaper:
    """
    Iterates through NNE CSV rows, returning the current upload
    speed in Mbps. Advances to the next row every update_interval_seconds.
    """

    def __init__(self, csv_path: str, rate_column: str, update_interval_seconds: int):
        self._rates = self._load_csv(csv_path, rate_column)
        self._update_interval = update_interval_seconds
        self._index = 0
        self._last_update = time.time()
        log.info(
            f"NNE shaper loaded {len(self._rates)} rate samples "
            f"from {csv_path}. Update interval: {update_interval_seconds}s"
        )

    def _load_csv(self, csv_path: str, rate_column: str) -> list:
        rates = []
        with open(csv_path, "r") as f:
            # Your CSV uses semicolon delimiter
            reader = csv.reader(f, delimiter=";")
            # Skip first row (node name) and read headers from second row
            next(reader)
            headers = next(reader)
            col_idx = headers.index(rate_column)
            for row in reader:
                try:
                    val = float(row[col_idx])
                    if val > 0:
                        rates.append(val)
                except (ValueError, IndexError):
                    continue
        if not rates:
            raise ValueError(f"No valid rates found in column '{rate_column}'")
        return rates

    def current_rate_mbps(self) -> float:
        """Return current upload rate in Mbps, advancing on schedule."""
        now = time.time()
        if now - self._last_update >= self._update_interval:
            self._index = (self._index + 1) % len(self._rates)
            self._last_update = now
            log.debug(
                f"NNE rate updated → {self._rates[self._index]:.2f} Mbps "
                f"(row {self._index}/{len(self._rates)})"
            )
        return self._rates[self._index]

    def current_rate_bytes_per_sec(self) -> float:
        """Return current upload rate in bytes/second."""
        mbps = self.current_rate_mbps()
        return (mbps * 1_000_000) / 8
