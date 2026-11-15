"""
IoT VNF Platform — Simulator Entry Point

Orchestrates multiple software IoT gateways dynamically
based on time of day, day of week, and the NNE 4G/5G dataset.

Usage:
  python main.py
  python main.py --config config/simulation.yaml
"""
import argparse
import logging
import multiprocessing
import signal
import sys
import time
import uuid

import yaml

from gateway import Gateway
from scheduler import get_current_scale


def setup_logging(level: str):
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def load_config(path: str) -> dict:
    with open(path, "r") as f:
        return yaml.safe_load(f)


def run_gateway(gateway_id: str, num_devices: int, config: dict):
    """
    Target function for each gateway process.
    Each gateway runs in its own process for true parallelism.
    """
    gw = Gateway(gateway_id, num_devices, config)
    gw.start()


def main():
    parser = argparse.ArgumentParser(description="IoT VNF Platform Simulator")
    parser.add_argument(
        "--config",
        default="config/simulation.yaml",
        help="Path to simulation config file",
    )
    args = parser.parse_args()

    config = load_config(args.config)
    setup_logging(config["logging"]["level"])

    log = logging.getLogger("main")
    log.info("=" * 60)
    log.info("IoT VNF Platform — Simulator Starting")
    log.info(f"Config: {args.config}")
    log.info(f"Scaling mode: {config['scaling']['mode']}")
    log.info("=" * 60)

    # Track running gateway processes
    active_processes = []
    current_scenario = None

    def shutdown(signum, frame):
        log.info("Shutdown signal received. Stopping all gateways...")
        for p in active_processes:
            p.terminate()
            p.join()
        sys.exit(0)

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)

    cycle_duration = config["gateway"]["cycle_duration_seconds"]

    while True:
        # Get the scale for current time
        num_gateways, devices_per_gw, scenario_name = get_current_scale(config)

        # Only restart gateways if scale changed
        if scenario_name != current_scenario:
            log.info(
                f"Scale change: '{current_scenario}' → '{scenario_name}' | "
                f"Gateways: {num_gateways} | "
                f"Devices/GW: {devices_per_gw} | "
                f"Total devices: {num_gateways * devices_per_gw:,}"
            )

            # Stop existing gateways
            for p in active_processes:
                p.terminate()
                p.join()
            active_processes.clear()

            # Start new gateways
            for i in range(num_gateways):
                gateway_id = f"gw-{i+1:02d}"
                p = multiprocessing.Process(
                    target=run_gateway,
                    args=(gateway_id, devices_per_gw, config),
                )
                p.start()
                active_processes.append(p)
                log.info(f"Started gateway {gateway_id} (PID {p.pid})")

            current_scenario = scenario_name

        # Wait one cycle before checking scale again
        time.sleep(cycle_duration)


if __name__ == "__main__":
    main()
