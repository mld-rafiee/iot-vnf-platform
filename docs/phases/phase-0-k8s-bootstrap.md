# Phase 0 — Kubernetes Bootstrap

## Status: Complete ✅

## What was done
- Prepared all 9 nodes (swap off, kernel modules, containerd)
- Installed kubeadm v1.31.14 on all nodes
- Initialized control plane on master (158.39.74.183)
- Installed Calico CNI (pod-network-cidr: 192.168.0.0/16)
- Joined all 8 workers to the cluster
- Labeled all nodes by role

## Node Map
| Node | IP | Role | VNF |
|------|----|------|-----|
| master | 158.39.74.183 | control-plane | - |
| worker-1 | 158.39.201.139 | vnf | firewall-1 |
| worker-2 | 158.39.77.61 | vnf | firewall-2 |
| worker-3 | 158.37.65.211 | vnf | dpi |
| worker-4 | 158.39.74.172 | vnf | encryption |
| worker-5 | 158.39.74.232 | vnf | compression |
| worker-6 | 158.39.74.241 | vnf | nat |
| tool-node-1 | 158.37.65.177 | data-engineering | - |
| edge-node | 158.37.65.44 | edge | - |

## Verify
kubectl get nodes
