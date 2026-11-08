# IoT VNF Platform

A full-stack IoT data engineering testbed built on Kubernetes,
using Virtual Network Functions (VNFs) as the data processing layer.

Designed as a reproducible platform for researchers and developers
to study real-world IoT data flows, SFC-based processing, and
cloud-native data engineering.

## Architecture
cat > README.md << 'EOF'
# IoT VNF Platform

A full-stack IoT data engineering testbed built on Kubernetes,
using Virtual Network Functions (VNFs) as the data processing layer.

Designed as a reproducible platform for researchers and developers
to study real-world IoT data flows, SFC-based processing, and
cloud-native data engineering.

## Architecture

EDGE NODE CLOUD (NREC / Kubernetes)
───────────────── ──────────────────────────────────────────
IoT Simulator
IoT Gateway ──4G/5G──► SFC1: FW1 → DPI → ENC → COMP
Telegraf SFC2: FW2 → DPI → NAT
↓
Kafka → MinIO (data lake)
→ ClickHouse (warehouse)
↓
dbt + Airflow (ELT pipeline)
Prometheus + Grafana (monitoring)


## SFC Topology

| Chain | Path | Note |
|-------|------|------|
| SFC1 | `Firewall-1 → DPI → Encryption → Compression` | Security + bandwidth path |
| SFC2 | `Firewall-2 → DPI → NAT` | Inter-domain routing path |

DPI is shared between both chains.

## Node Map

| Node | Flavor | Role |
|------|--------|------|
| master | c1.2xlarge | K8s control plane + Kafka + Prometheus + Grafana + MinIO + ArgoCD |
| worker-1 | c1.medium | VNF: Firewall-1 |
| worker-2 | c1.medium | VNF: Firewall-2 |
| worker-3 | c1.medium | VNF: DPI (shared) |
| worker-4 | c1.medium | VNF: Encryption |
| worker-5 | c1.medium | VNF: Compression |
| worker-6 | c1.medium | VNF: NAT |
| tool-node-1 | c1.xlarge | ClickHouse + Airflow + dbt + Flink |
| edge-node | c1.medium | IoT Simulator + Gateway + Telegraf |

## Build Phases

| Phase | Description | Status |
|-------|-------------|--------|
| 0 | K8s bootstrap + ArgoCD | 🔄 In progress |
| 1 | Kafka + IoT data flow | ⏳ Pending |
| 2 | VNF chain deployment | ⏳ Pending |
| 3 | Data lake + warehouse | ⏳ Pending |
| 4 | Monitoring | ⏳ Pending |
| 5 | ELT pipeline (dbt + Airflow) | ⏳ Pending |
| 6 | Stream processing + data quality | ⏳ Pending |
| 7 | CI/CD + IaC | ⏳ Pending |

## Tech Stack

| Layer | Tool |
|-------|------|
| Orchestration | Kubernetes (kubeadm) |
| GitOps | ArgoCD |
| Message broker | Kafka (Strimzi) |
| Data lake | MinIO |
| Warehouse | ClickHouse |
| Batch transform | dbt |
| Orchestration | Apache Airflow |
| Stream processing | Apache Flink |
| Monitoring | Prometheus + Grafana |
| Data quality | Great Expectations |
| CI/CD | GitHub Actions |
| IaC | Terraform + Ansible + Helm |

## Infrastructure

- **Cloud:** NREC (Norwegian Research and Education Cloud)
- **Orchestration:** Kubernetes (vanilla, kubeadm)
- **Nodes:** 9 total

## Quick Start

> Coming after Phase 7 — full one-command deployment

## License

MIT
