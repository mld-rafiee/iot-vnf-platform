FROM python:3.11-slim

# Install nftables — the real Linux firewall engine
# Required for stateful connection tracking and rule application
RUN apt-get update && apt-get install -y \
    nftables \
    iproute2 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy shared VNF modules
COPY vnfs/shared/base_vnf.py   .
COPY vnfs/shared/metrics.py    .
COPY vnfs/shared/vnf_memory.py .

# Copy firewall-specific files
COPY vnfs/firewall/main.py              .
COPY vnfs/firewall/generate_rules.py    .
COPY vnfs/firewall/config/rules.json    config/rules.json

COPY vnfs/firewall/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Prometheus metrics — one port per worker process (NUM_WORKERS=4)
# worker-0: 8000, worker-1: 8001, worker-2: 8002, worker-3: 8003
EXPOSE 8000 8001 8002 8003

# Kubernetes health probe port (main process)
EXPOSE 8010

CMD ["python", "main.py"]
