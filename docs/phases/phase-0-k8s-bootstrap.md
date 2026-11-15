# Phase 0 — Kubernetes Bootstrap + ArgoCD

## Status: Complete ✅

## What Was Built

### Kubernetes Cluster
- 9-node cluster bootstrapped with kubeadm on NREC
- Kubernetes version: v1.31.14
- CNI: Calico in VXLAN mode (required for cross-subnet routing between NREC subnets)

### Node Map
| Node | Flavor | Role |
|------|--------|------|
| master | c1.2xlarge | K8s control plane |
| worker-1 | c1.medium | VNF: Firewall-1 |
| worker-2 | c1.medium | VNF: Firewall-2 |
| worker-3 | c1.medium | VNF: DPI (shared) |
| worker-4 | c1.medium | VNF: Encryption |
| worker-5 | c1.medium | VNF: Compression |
| worker-6 | c1.medium | VNF: NAT |
| tool-node-1 | c1.xlarge | Data engineering tools |
| edge-node | c1.xlarge | Edge: IoT Simulator |

### GitOps — ArgoCD
- Installed in `argocd` namespace
- Watches `develop` branch automatically
- Every merge to `develop` triggers auto-deploy
- Access: `kubectl port-forward svc/argocd-server -n argocd 8080:443`

## Verify

```bash
kubectl get nodes
kubectl get pods -n argocd
kubectl get applications -n argocd
```
