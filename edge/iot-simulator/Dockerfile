FROM python:3.11-slim

WORKDIR /app

# Install dependencies first (Docker layer caching)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY . .

# Default config path (can be overridden via K8s ConfigMap)
ENV CONFIG_PATH=config/simulation.yaml

CMD ["python", "main.py", "--config", "config/simulation.yaml"]
