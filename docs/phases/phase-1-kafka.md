# Phase 1 — Kafka + IoT Simulator

## Status: Complete ✅

## What Was Built

### Kafka Cluster (Strimzi)
- Deployed using Strimzi operator on Kubernetes
- KRaft mode (no ZooKeeper)
- Single broker on master node
- Kafka version: 4.3.2

### Topics
| Topic | Partitions | Retention | Purpose |
|-------|-----------|-----------|---------|
| `iot-raw` | 3 | 7 days | Raw sensor data → data lake |
| `sfc1-ingress` | 3 | 7 days | Data entering SFC1 |
| `sfc1-egress` | 3 | 7 days | Data exiting SFC1 |
| `sfc2-ingress` | 3 | 7 days | Data entering SFC2 |
| `sfc2-egress` | 3 | 7 days | Data exiting SFC2 |

### Kafka UI
- Access: `kubectl port-forward svc/kafka-ui -n kafka 8081:8080`
- URL: `http://localhost:8081`

### IoT Simulator
Deployed on `edge-node` (c1.xlarge: 4 vCPU, 8GB RAM).

#### Architecture

edge-node
├── main.py — entry point, reads config, manages gateways
├── scheduler.py — determines scale based on time of day
├── gateway.py — one gateway process per active gateway
│ ├── N device threads (environmental, industrial, network sensors)
│ ├── Shared multiprocessing.Queue
│ └── 4 producer processes → confluent-kafka → Kafka
└── nne.py — NNE 4G/5G bandwidth shaper


#### Sensor Types
| Type | Target Topic | Real-world analog |
|------|-------------|-------------------|
| Environmental (temp, humidity, CO2) | sfc1-ingress | Smart building |
| Industrial (vibration, pressure, power) | sfc1-ingress | Factory floor |
| Network (packet loss, latency, throughput) | sfc2-ingress | Network probes |

#### Dynamic Scaling (weekday)
| Time | Scenario | Gateways | Total Devices |
|------|----------|----------|---------------|
| 00-05 | Night | 1 | 1,000 |
| 06-08 | Morning ramp | 10 | 10,000 |
| 09-16 | Business peak | 25 | 25,000 |
| 17-19 | Evening shoulder | 10 | 10,000 |
| 20-23 | Night wind-down | 1 | 1,000 |

#### NNE Bandwidth Shaping
Sensor data is transmitted at rates governed by real 4G/5G upload speed
measurements from the NorNet Edge (NNE) dataset, collected across Norway
between November 2023 and February 2024. The rate updates every 60 seconds.

- Achieved TX rate: ~29-31 Mbps (NNE average target: ~31.68 Mbps)

#### Message Format
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

## Verify

```bash
kubectl get kafka -n kafka
kubectl get kafkatopics -n kafka
kubectl get pods -n iot
kubectl logs -n iot -l app=iot-simulator --since=5s | grep "TX rate"
kubectl port-forward svc/kafka-ui -n kafka 8081:8080
```
