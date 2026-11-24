FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    snort \
    libpcap-dev \
    && rm -rf /var/lib/apt/lists/*

RUN ln -sf /usr/bin/python3 /usr/bin/python

WORKDIR /app

COPY vnfs/shared/base_vnf.py   .
COPY vnfs/shared/metrics.py    .
COPY vnfs/shared/vnf_memory.py .

COPY vnfs/dpi/main.py               .
COPY vnfs/dpi/config/snort.lua      /etc/snort/snort.lua
COPY vnfs/dpi/config/snort.conf     /etc/snort/snort.conf
COPY vnfs/dpi/config/rules/         /etc/snort/rules/

COPY vnfs/dpi/requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

# snort apt package already creates snort user/group and /etc/snort
# Just create missing dirs and set permissions
RUN mkdir -p /var/log/snort \
    && mkdir -p /etc/snort/lists \
    && touch /etc/snort/lists/white_list.rules \
    && touch /etc/snort/lists/black_list.rules \
    && chown -R snort:snort /var/log/snort /etc/snort

EXPOSE 8000 8001 8002 8003
EXPOSE 8010

CMD ["python", "main.py"]
