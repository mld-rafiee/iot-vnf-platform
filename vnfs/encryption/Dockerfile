FROM python:3.11-slim

WORKDIR /app

# Copy shared VNF modules
COPY vnfs/shared/base_vnf.py   .
COPY vnfs/shared/metrics.py    .
COPY vnfs/shared/vnf_memory.py .

# Copy encryption-specific files
COPY vnfs/encryption/main.py          .
COPY vnfs/encryption/requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

# Prometheus metrics ports (one per worker)
EXPOSE 8000 8001 8002 8003

# Health port
EXPOSE 8010

CMD ["python", "main.py"]
