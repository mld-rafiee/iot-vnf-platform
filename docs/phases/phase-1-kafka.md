# Phase 1 - Kafka + IoT Simulator

## Overview

Phase 1 builds the data ingestion layer of the testbed. By the end of
this phase, simulated IoT sensor data flows from the edge node through
a 4G/5G network emulation layer into Kafka topics, ready to be consumed
by the VNF chain in Phase 2.

By the end of this phase you will have:

- Kafka running inside Kubernetes (using the Strimzi operator)
- Five Kafka topics representing the data flow through the SFC topology
- A Kafka UI dashboard for real-time message inspection
- An IoT simulator running on the edge node, generating realistic
  sensor data from multiple device types
- Real 4G/5G bandwidth shaping based on the NorNet Edge (NNE) dataset
- Dynamic scaling that changes the number of active gateways and devices
  based on time of day, matching real-world IoT traffic patterns

---

## Background: Why Kafka?

In a real IoT deployment, thousands of sensors continuously generate data.
This data must be collected, buffered, and distributed to multiple consumers
(VNFs, storage, analytics) simultaneously.

Kafka is a distributed message broker designed for exactly this. It acts
as a central hub where producers (IoT gateways) publish data and consumers
(VNFs, databases) read it independently at their own pace.

Key properties that make Kafka suitable for this testbed:

**Decoupling** - Producers and consumers are independent. If a VNF is
slow or temporarily down, messages accumulate in Kafka and are processed
when the VNF recovers. No data is lost.

**Multiple consumers** - The same message can be read by multiple consumers.
In this testbed, every message goes to both the SFC topic (for VNF processing)
and the `iot-raw` topic (for the data lake), without the producer needing
to send it twice at the application level.

**Retention** - Kafka stores messages for a configurable period (7 days
in this testbed). This allows replaying historical data for debugging
or reprocessing.

**Partitioning** - Each topic is split into partitions that can be
processed in parallel. With 3 partitions per topic, three consumers
can process data simultaneously.

---

## Background: What is Strimzi?

Running Kafka manually on Kubernetes requires managing ZooKeeper,
brokers, persistent volumes, certificates, and rolling upgrades — all
complex and error-prone.

Strimzi is a Kubernetes operator that manages Kafka for you. An operator
is a piece of software that runs inside Kubernetes and knows how to
install, configure, scale, and recover another piece of software.

With Strimzi, you describe the Kafka cluster you want in a YAML file
and Strimzi creates and maintains it. You never manually configure
Kafka properties or restart brokers.

This testbed uses Strimzi in KRaft mode. KRaft is Kafka's built-in
consensus protocol that replaces ZooKeeper. It simplifies the
architecture (fewer components) and is the direction Kafka is moving.

---

## Background: IoT Simulator Design

The IoT simulator faithfully reproduces the traffic generation
architecture described in the paper:

- Software IoT devices generate JSON sensor messages at configurable rates
- Software IoT gateways aggregate messages into batches of 4000
- Gateways transmit batches at rates governed by real 4G/5G measurements
  from the NorNet Edge (NNE) dataset

The NNE dataset contains upload speed measurements collected across
Norway between November 2023 and February 2024 from operational 4G
and 5G networks. Using real measurement data instead of synthetic
bandwidth profiles ensures the testbed reflects the dynamic variability
of real deployments.

The simulator scales dynamically based on time of day, following
observed real-world IoT traffic patterns where activity peaks during
business hours and drops at night.

---

## Prerequisites

- Phase 0 complete (Kubernetes cluster running, ArgoCD configured)
- Docker installed on your local machine
- GitHub Container Registry access (ghcr.io)
- The NNE dataset CSV file (see Data section below)

---

## Data: NNE Dataset

The simulator requires the NorNet Edge dataset file `node_950_C.csv`.
This file is not included in the repository because it is research data.

Expected file location: `edge/iot-simulator/config/node_950_C.csv`

Expected CSV format (semicolon-delimited):
```
node_950
ts;node_id;network_id;...;speedtest_ul_mbps;speedtest_dl_mbps;...
2023-11-01 00:00:00;950;2;...;31.68;89.41;...
```

The simulator reads the `speedtest_ul_mbps` column and uses each value
as the transmission rate for 60 seconds before advancing to the next row.

See `edge/iot-simulator/config/node_950_C.csv.example` for the full
expected format.

---

## Step 1 - Install Strimzi Operator

The Strimzi operator must be installed before creating a Kafka cluster.
The operator watches for Kafka custom resources and manages the actual
Kafka pods.

```bash
kubectl create namespace kafka

kubectl create \
    -f 'https://strimzi.io/install/latest?namespace=kafka' \
    -n kafka

# Wait for the operator pod to be Running
kubectl get pods -n kafka -w
```

You should see one pod: `strimzi-cluster-operator-xxx` with status Running.

Verify the Kafka CRDs were installed:

```bash
kubectl get crd | grep kafka
# Should list: kafkas, kafkatopics, kafkausers, kafkanodepools, etc.
```

---

## Step 2 - Deploy Kafka Cluster

With the operator running, create a Kafka cluster by applying the
manifest at `k8s/kafka/kafka-cluster.yaml`.

This testbed uses a single-broker KRaft cluster. KRaft combines the
controller and broker roles in one node (called `dual-role`), eliminating
the need for a separate ZooKeeper deployment.

The manifest defines:

- **KafkaNodePool** - specifies the node(s) with both `controller` and
  `broker` roles, storage type, and replica count
- **Kafka** - the cluster configuration including listeners, Kafka version,
  replication settings, and the entity operator

The entity operator manages KafkaTopic and KafkaUser resources
automatically — when you apply a KafkaTopic YAML, the entity operator
creates it in Kafka without you needing to run any `kafka-topics.sh` commands.

```bash
kubectl apply -f k8s/kafka/kafka-cluster.yaml

# Watch the pods appear (takes 1-2 minutes)
kubectl get pods -n kafka -w
```

Expected pods:
```
iot-kafka-dual-role-0              1/1  Running
iot-kafka-entity-operator-xxx      2/2  Running
strimzi-cluster-operator-xxx       1/1  Running
```

Verify the cluster is Ready:

```bash
kubectl get kafka -n kafka
# NAME        READY   KAFKA VERSION   METADATA VERSION
# iot-kafka   True    4.3.2           4.3-IV0
```

---

## Step 3 - Create Topics

Topics are the channels through which data flows. Each topic maps to
a stage in the SFC processing pipeline.

```bash
kubectl apply -f k8s/kafka/kafka-topics.yaml
```

This creates five topics:

| Topic | Purpose |
|-------|---------|
| `iot-raw` | All raw sensor messages, written by every gateway. Used as the data lake ingestion point. |
| `sfc1-ingress` | Environmental and industrial sensor data entering SFC1 (Firewall-1 -> DPI -> Encryption -> Compression) |
| `sfc1-egress` | Processed data exiting SFC1, ready for storage |
| `sfc2-ingress` | Network sensor data entering SFC2 (Firewall-2 -> DPI -> NAT) |
| `sfc2-egress` | Processed data exiting SFC2, ready for storage |

Each topic has 3 partitions, allowing up to 3 consumers to process
data from the same topic in parallel.

Verify all topics are Ready:

```bash
kubectl get kafkatopics -n kafka
```

---

## Step 4 - Install Kafka UI

Kafka UI is a web dashboard that shows topics, messages, consumers,
and broker health in real time. It is essential for verifying that
messages are flowing correctly and for debugging.

```bash
kubectl apply -f k8s/kafka/kafka-ui.yaml

# Wait for the pod to be Running
kubectl get pods -n kafka | grep kafka-ui
```

Access the dashboard:

```bash
kubectl port-forward svc/kafka-ui -n kafka 8081:8080
# Open http://localhost:8081
```

In the dashboard you can:

- See all topics and their message counts
- Browse individual messages including their key, value, and timestamp
- Monitor consumer group lag (how far behind a consumer is)
- Watch messages arrive in real time using Live mode

---

## Step 5 - Build and Push the IoT Simulator Image

The IoT simulator is a Python application that runs on the edge node.
It must be packaged as a Docker image and pushed to a container registry
so Kubernetes can pull and run it.

This testbed uses GitHub Container Registry (ghcr.io), which is free
for public repositories.

The simulator source code is in `edge/iot-simulator/`.

**Important:** Always build for linux/amd64 regardless of your local
machine architecture. Cloud VMs typically run on AMD64. If you build
on an Apple Silicon Mac without specifying the platform, the image will
be ARM64 and will fail to run on the nodes.

```bash
cd edge/iot-simulator

docker buildx build \
    --platform linux/amd64 \
    -t ghcr.io/<YOUR_GITHUB_USERNAME>/iot-vnf-platform/iot-simulator:latest \
    --push \
    .
```

Make the package public on GitHub:
GitHub -> your profile -> Packages -> iot-simulator -> Package settings
-> Change visibility -> Public

This allows the edge node to pull the image without credentials.

---

## Step 6 - Configure the Simulator

All simulator parameters are in `edge/iot-simulator/config/simulation.yaml`.

Key parameters:

```yaml
sensor:
  rates_per_second: [4, 12, 25, 50]  # each device randomly picks one
  batch_size: 4000                    # messages per batch (from paper)

nne:
  update_interval_seconds: 60  # how often to advance to next NNE row

scaling:
  mode: "dynamic"   # "dynamic" for time-based, "fixed" for constant load

  fixed:            # used when mode: fixed
    gateways: 1
    devices_per_gateway: 1000

  dynamic:
    weekday:
      - name: "night"
        hours: [0, 1, 2, 3, 4, 5]
        gateways: 1
        devices_per_gateway: 1000
      # ... more time slots
```

To run with a fixed number of devices for controlled experiments,
set `mode: "fixed"` and specify `gateways` and `devices_per_gateway`.

---

## Step 7 - Deploy the IoT Simulator

The simulator deployment is at `k8s/iot/iot-simulator-deployment.yaml`.

It is pinned to the edge node using `nodeSelector`:

```yaml
nodeSelector:
  kubernetes.io/hostname: edge-node
```

This ensures the simulator always runs on the designated edge node,
not on a VNF worker node.

Merge to `develop` to trigger ArgoCD deployment:

```bash
git add .
git commit -m "feat: deploy iot simulator"
git push origin phase/1-kafka

git checkout develop
git merge phase/1-kafka --no-edit
git push origin develop
```

ArgoCD will detect the change and deploy within 3 minutes. Watch:

```bash
kubectl get pods -n iot -w
```

---

## Simulator Architecture

Understanding how the simulator works internally helps when tuning
parameters or debugging.

```
edge-node
|
+-- main.py
|   Reads simulation.yaml. Checks current time and day of week.
|   Determines how many gateways to run. Starts one process per gateway.
|   Checks every cycle_duration_seconds and restarts if scale changes.
|
+-- scheduler.py
|   Returns (num_gateways, devices_per_gateway, scenario_name)
|   based on current hour and weekday/weekend schedule in config.
|
+-- gateway.py (runs once per active gateway)
|   |
|   +-- Device threads (N threads, one per IoT device)
|   |   Each thread generates messages at its configured rate
|   |   and puts them into a shared Queue.
|   |
|   +-- Producer processes (4 parallel processes)
|       Each process reads from the shared Queue, batches messages,
|       and sends to Kafka using confluent-kafka (C library).
|       Rate is limited by the current NNE bandwidth value.
|
+-- nne.py
    Reads node_950_C.csv. Returns current upload speed in Mbps.
    Advances to next row every update_interval_seconds.
```

**Why multiprocessing instead of threading?**

Python has the Global Interpreter Lock (GIL) which prevents true
parallel execution across threads. With 1000 devices generating 20,000
messages per second, JSON serialization becomes CPU-bound. Using 4
separate processes bypasses the GIL and each process gets its own CPU
core, achieving sufficient throughput to match NNE target rates.

**Why confluent-kafka instead of kafka-python?**

`kafka-python` is a pure Python implementation and is therefore
GIL-bound, limiting throughput to around 7-9 Mbps per process.
`confluent-kafka` wraps librdkafka, a battle-tested C library, and
achieves 2-5x higher throughput by bypassing the GIL for network I/O.

---

## Sensor Types and SFC Routing

Three sensor categories are simulated, each routing to a different
SFC based on its data characteristics:

**Environmental sensors** (temperature, humidity, CO2)
- Real-world analog: smart building, smart home
- Target topic: `sfc1-ingress`
- Routed through SFC1: Firewall -> DPI -> Encryption -> Compression
- Rationale: building sensor data contains personally identifiable
  information and requires encryption and compression before storage

**Industrial sensors** (vibration, pressure, power consumption)
- Real-world analog: factory floor, industrial equipment
- Target topic: `sfc1-ingress`
- Routed through SFC1: same security and compression requirements

**Network sensors** (packet loss, latency, throughput)
- Real-world analog: network probes, edge routers, 5G base stations
- Target topic: `sfc2-ingress`
- Routed through SFC2: Firewall -> DPI -> NAT
- Rationale: network telemetry data is routed through the NAT chain
  for inter-domain forwarding

All sensor types also write to `iot-raw` for the data lake.

---

## Message Format

Every sensor message follows this JSON structure, matching the format
used in the original paper:

```json
{
  "sensor_id": "env-gw-01-a3f2-temperature",
  "name": "Environmental Temperature Sensor",
  "type": "temperature",
  "unit": "celsius",
  "value": 23.4,
  "timestamp": 1731234567.891,
  "gateway_id": "gw-01",
  "sfc_target": "sfc1"
}
```

The `sfc_target` field tells downstream components which SFC chain
this message belongs to.

---

## Dynamic Scaling Schedule

The simulator follows real-world IoT traffic patterns documented in
research on enterprise IoT deployments (Finley & Vesselkov, 2019;
Hajhussein & Ibnkahla, 2024). Industrial and commercial IoT traffic
peaks during business hours and halves on weekends.

| Day | Time | Scenario | Gateways | Total Devices |
|-----|------|----------|----------|---------------|
| Weekday | 00-05 | Night | 1 | 1,000 |
| Weekday | 06-08 | Morning ramp | 10 | 10,000 |
| Weekday | 09-16 | Business peak | 25 | 25,000 |
| Weekday | 17-19 | Evening shoulder | 10 | 10,000 |
| Weekday | 20-23 | Night wind-down | 1 | 1,000 |
| Weekend | 00-09, 20-23 | Weekend low | 1 | 1,000 |
| Weekend | 10-19 | Weekend mid | 10 | 10,000 |

These four scales (1K, 10K, 25K, 50K devices) correspond to the
small, medium, large, and very large scenarios in the paper.

---

## Verify

```bash
# Kafka cluster healthy
kubectl get kafka -n kafka

# All topics Ready
kubectl get kafkatopics -n kafka

# Simulator running on edge-node
kubectl get pods -n iot -o wide

# Messages flowing - check TX rate
kubectl logs -n iot -l app=iot-simulator --since=5s | grep "TX rate"

# Combined TX rate across all producer processes
kubectl logs -n iot -l app=iot-simulator --since=5s 2>&1 \
  | grep "TX rate" \
  | awk -F'TX rate: ' '{print $2}' \
  | awk -F' Mbps' '{print $1}' \
  | sort -n | tail -4 \
  | awk '{sum += $1} END {print "Total TX: " sum " Mbps"}'

# Open Kafka UI to see messages arriving
kubectl port-forward svc/kafka-ui -n kafka 8081:8080
# Open http://localhost:8081
```

Expected TX rate: approximately 29-31 Mbps (NNE average target: ~31.68 Mbps).
The rate follows the NNE dataset and changes every 60 seconds.