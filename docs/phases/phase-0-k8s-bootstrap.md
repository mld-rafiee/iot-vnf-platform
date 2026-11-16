# Phase 0 - Kubernetes Bootstrap + ArgoCD

## Overview

Phase 0 sets up the full infrastructure foundation for the testbed.
Everything in Phase 1 and beyond depends on this phase being complete.

By the end of this phase you will have:

- A 9-node Kubernetes cluster running on a cloud provider
- Pod networking working across all nodes (including across subnets)
- kubectl working from your local machine
- ArgoCD running inside the cluster, watching your GitHub repository,
  and automatically deploying every change you merge to `develop`

---

## Prerequisites

- A cloud provider account with VM provisioning capability
  (this testbed was built and tested on NREC - https://www.nrec.no/)
- Ubuntu 26.04 LTS on all nodes
- SSH key pair for node access
- Local machine with:
  - `kubectl` installed (https://kubernetes.io/docs/tasks/tools/)
  - `git` installed
  - Docker installed (for building images in later phases)

---

## Node Requirements

This testbed uses 9 nodes. Each node has a single dedicated role.
No role sharing — one node, one purpose.

| Node | Min Spec | Role |
|------|----------|------|
| master | 8 vCPU, 16GB RAM | Kubernetes control plane |
| worker-1 | 1 vCPU, 2GB RAM | VNF: Firewall-1 |
| worker-2 | 1 vCPU, 2GB RAM | VNF: Firewall-2 |
| worker-3 | 1 vCPU, 2GB RAM | VNF: DPI (shared) |
| worker-4 | 1 vCPU, 2GB RAM | VNF: Encryption |
| worker-5 | 1 vCPU, 2GB RAM | VNF: Compression |
| worker-6 | 1 vCPU, 2GB RAM | VNF: NAT |
| tool-node-1 | 4 vCPU, 8GB RAM | Data engineering tools |
| edge-node | 4 vCPU, 8GB RAM | IoT Simulator + Gateway |

The master node is larger because it runs the Kubernetes control plane
components plus the data engineering tools (Kafka, Prometheus, MinIO, ArgoCD).

The worker nodes are small because each runs only a single VNF microservice.
This mirrors a real-world Network Function Virtualization deployment where
each VNF gets dedicated compute resources.

### Network Requirements

If your nodes span multiple subnets (common in cloud environments where
VMs are spread across availability zones), Calico must be configured in
VXLAN mode. This is covered in Step 4. Single-subnet deployments work
with the default Calico settings.

### Firewall/Security Group Requirements

Your cloud provider's firewall must allow the following traffic:

| Direction | Protocol | Port | Source | Purpose |
|-----------|----------|------|--------|---------|
| Ingress | TCP | 22 | 0.0.0.0/0 | SSH access to nodes |
| Ingress | TCP | 6443 | 0.0.0.0/0 | Kubernetes API server |
| Ingress | UDP | 4789 | 0.0.0.0/0 | VXLAN pod networking (Calico) |
| Ingress | TCP | Any | internal subnet(s) | Cross-node Kubernetes traffic |
| Egress | Any | Any | 0.0.0.0/0 | Nodes pulling images and packages |

Port 6443 is the Kubernetes API server. kubectl on your local machine
talks to this port to manage the cluster.

Port 4789 is used by Calico VXLAN to tunnel pod traffic between nodes.
Without this, pods on different nodes cannot communicate.

---

## Step 1 - Prepare All Nodes

Before Kubernetes can run, every node needs the same base configuration.
The script `scripts/k8s-node-setup.sh` in this repository handles all of it.

What the script does and why:

**System updates** - ensures all security patches are applied before
installing Kubernetes components.

**Disable swap** - Kubernetes manages memory allocation for pods itself.
If swap is enabled, the OS can silently move memory to disk, which
breaks Kubernetes memory accounting. K8s refuses to start if swap is on.

**Load kernel modules** - Two modules are required:
- `overlay` - allows containers to have layered filesystems (needed by containerd)
- `br_netfilter` - allows Kubernetes to see and filter network traffic
  crossing the bridge between pods

**Configure kernel network settings** - Tells the Linux kernel to allow
iptables to see bridged traffic (required for pod-to-pod routing) and
to forward packets between network interfaces (required for pod routing).

**Install containerd** - Kubernetes does not run containers directly.
It delegates that to a container runtime. `containerd` is the industry
standard runtime used in production Kubernetes clusters.

**Install kubeadm, kubelet, kubectl** - The three core Kubernetes tools:
- `kubeadm` - bootstraps the cluster (used once during setup)
- `kubelet` - the agent that runs on every node and manages pods
- `kubectl` - the command line tool for managing the cluster

**Version pinning** - All three are pinned to v1.31.14 to prevent
accidental upgrades. Kubernetes upgrades must be done carefully and
deliberately, not automatically.

```bash
bash scripts/k8s-node-setup.sh
```

Run on master first, then copy and run on all other nodes. The script
is idempotent - safe to run multiple times.

---

## Step 2 - Initialize Kubernetes Control Plane

The control plane is the brain of the cluster. It runs on the master node
and consists of:

- **kube-apiserver** - the central API that everything talks to
- **etcd** - the cluster database storing all cluster state
- **kube-scheduler** - decides which node a new pod runs on
- **kube-controller-manager** - watches cluster state and fixes drift

`kubeadm init` starts all of these automatically.

Run on master only:

```bash
sudo kubeadm init \
  --pod-network-cidr=192.168.0.0/16 \
  --apiserver-advertise-address=<MASTER_IP>
```

`--pod-network-cidr=192.168.0.0/16` reserves this IP range for pods.
Calico (installed in Step 4) will assign pod IPs from this range.

`--apiserver-advertise-address` tells other nodes which IP to use when
connecting to the control plane. Use the master's public IP.

After init completes, configure kubectl on master:

```bash
mkdir -p $HOME/.kube
sudo cp -i /etc/kubernetes/admin.conf $HOME/.kube/config
sudo chown $(id -u):$(id -g) $HOME/.kube/config
```

This copies the cluster credentials to the standard kubectl location.

**Important:** Save the join command printed at the end of kubeadm init.
It looks like this and expires after 24 hours:

```
kubeadm join <MASTER_IP>:6443 --token <TOKEN> \
    --discovery-token-ca-cert-hash sha256:<HASH>
```

---

## Step 3 - Join Worker Nodes

Each worker node joins the cluster by running the join command from Step 2.
This tells the worker where the control plane is and proves it has the
right credentials.

Run on all 8 remaining nodes:

```bash
sudo kubeadm join <MASTER_IP>:6443 \
    --token <TOKEN> \
    --discovery-token-ca-cert-hash sha256:<HASH>
```

After joining, the worker registers itself with the control plane and
starts the kubelet agent. The node will appear in `kubectl get nodes`
but show `NotReady` until the network plugin is installed in Step 4.

---

## Step 4 - Install Calico CNI

**What is a CNI plugin?**

Kubernetes creates pods but deliberately does not handle networking
between them. It delegates that to a CNI (Container Network Interface)
plugin. Without a CNI plugin, pods cannot communicate with each other
at all, and all nodes stay in `NotReady` state.

Calico is the most widely used CNI plugin in production Kubernetes
clusters. It:

- Assigns IP addresses to pods from the `192.168.0.0/16` range
- Routes traffic between pods across different nodes
- Enforces NetworkPolicy rules (firewall rules between pods)
- Uses BGP (Border Gateway Protocol) to exchange pod routes between nodes

```bash
kubectl apply -f https://docs.projectcalico.org/manifests/calico.yaml
```

This deploys Calico as a DaemonSet - one Calico pod runs on every node.
Wait 60 seconds then verify all nodes are Ready:

```bash
kubectl get nodes
```

### Multi-Subnet Configuration

If your nodes span multiple subnets, you need an extra step.

By default, Calico uses IPIP (IP-in-IP) encapsulation to tunnel pod
traffic between nodes. IPIP wraps pod packets inside IP packets using
protocol number 4. Some cloud providers block protocol 4 at the
hypervisor level, regardless of firewall rules, causing 100% packet
loss between pods on different subnets.

VXLAN is an alternative encapsulation that wraps packets inside UDP.
UDP is universally allowed by cloud providers. Switch Calico to VXLAN:

```bash
kubectl patch ippool default-ipv4-ippool \
    --type=merge \
    --patch='{"spec":{"ipipMode":"Never","vxlanMode":"Always"}}'

kubectl rollout restart daemonset calico-node -n kube-system
kubectl rollout status daemonset calico-node -n kube-system
```

Verify cross-node pod communication before continuing:

```bash
# Get a pod IP from one subnet
kubectl get pods -n kube-system -o wide | grep coredns

# Ping it from a node on a different subnet
ssh <NODE_ON_OTHER_SUBNET> "ping -c 3 <POD_IP>"
# Expected: 0% packet loss
```

If you see 100% packet loss, check that UDP port 4789 is open in your
firewall and that VXLAN mode is active.

---

## Step 5 - Configure kubectl on Local Machine

Right now kubectl only works from the master node over SSH. To manage
the cluster from your local machine, copy the cluster credentials:

```bash
mkdir -p ~/.kube
scp ubuntu@<MASTER_IP>:~/.kube/config ~/.kube/config
```

This file contains the cluster CA certificate and an admin client
certificate that authenticates you to the API server.

Make sure port 6443 is open in your cloud firewall for your local IP.

Verify kubectl works from your local machine:

```bash
kubectl get nodes
# Should show all 9 nodes
```

---

## Step 6 - Label Nodes

Kubernetes nodes have no role labels by default (they show `<none>`).
Labels allow us to pin specific workloads to specific nodes using
`nodeSelector` in pod specs. This ensures each VNF always runs on its
dedicated node, mirroring a real NFV deployment.

```bash
# VNF-specific labels (used for nodeSelector in VNF deployments)
kubectl label node worker-1 node-role.kubernetes.io/vnf=firewall-1
kubectl label node worker-2 node-role.kubernetes.io/vnf=firewall-2
kubectl label node worker-3 node-role.kubernetes.io/vnf=dpi
kubectl label node worker-4 node-role.kubernetes.io/vnf=encryption
kubectl label node worker-5 node-role.kubernetes.io/vnf=compression
kubectl label node worker-6 node-role.kubernetes.io/vnf=nat

# Role labels (shown in kubectl get nodes ROLES column)
kubectl label node worker-1 worker-2 worker-3 \
    worker-4 worker-5 worker-6 kubernetes.io/role=vnf
kubectl label node tool-node-1 kubernetes.io/role=data-engineering \
    node-role.kubernetes.io/worker=data-engineering
kubectl label node edge-node kubernetes.io/role=edge \
    node-role.kubernetes.io/worker=edge
```

---

## Step 7 - Install ArgoCD

**What is ArgoCD?**

ArgoCD is a GitOps continuous delivery tool for Kubernetes. It runs
inside your cluster as a set of pods and watches a Git repository.
When it detects a change in the repository, it automatically applies
those changes to the cluster.

Without ArgoCD, deploying a change means:
1. Edit a YAML file
2. SSH into master
3. Run `kubectl apply`
4. Hope you didn't forget anything

With ArgoCD, deploying a change means:
1. Edit a YAML file
2. `git push`

ArgoCD handles the rest. The cluster always matches what is in Git.
This is called GitOps - Git is the single source of truth for your
infrastructure.

```bash
kubectl create namespace argocd

kubectl apply -n argocd \
    -f 'https://raw.githubusercontent.com/argoproj/argo-cd/stable/manifests/install.yaml'

# The ApplicationSet CRD is very large and exceeds kubectl's annotation
# size limit. Use server-side apply to bypass this limit.
kubectl apply \
    -f 'https://raw.githubusercontent.com/argoproj/argo-cd/stable/manifests/crds/applicationset-crd.yaml' \
    --server-side --force-conflicts

kubectl rollout restart deployment argocd-applicationset-controller -n argocd

# Wait for all pods to be Running (takes 1-2 minutes)
kubectl get pods -n argocd -w
```

Get the initial admin password (auto-generated during install):

```bash
kubectl get secret argocd-initial-admin-secret \
    -n argocd \
    -o jsonpath="{.data.password}" | base64 -d
```

Access the ArgoCD web UI:

```bash
kubectl port-forward svc/argocd-server -n argocd 8080:443
```

Open https://localhost:8080 in your browser. Accept the self-signed
certificate warning. Login with username `admin` and the password
from the command above. Change the password immediately after login.

### Connect Your GitHub Repository

ArgoCD needs to know which repository to watch. In the ArgoCD UI:

Settings -> Repositories -> Connect Repo

- Type: git
- URL: your GitHub repository URL
- Username: your GitHub username
- Password: GitHub Personal Access Token with `repo` scope
  (GitHub -> Settings -> Developer settings -> Personal access tokens)

A green checkmark confirms the connection succeeded.

### Create the ArgoCD Application

An ArgoCD Application tells ArgoCD which path in the repository to
watch and which cluster/namespace to deploy to.

```bash
kubectl apply -f k8s/argocd/iot-vnf-platform-app.yaml
```

This creates an Application that watches the `develop` branch of this
repository, scans the `k8s/` directory recursively, and automatically
syncs any changes to the cluster.

From this point forward, the deployment workflow is:
```
edit YAML -> git push -> merge to develop -> ArgoCD deploys automatically
```

---

## Verify Everything

Run these commands to confirm Phase 0 is complete:

```bash
# All 9 nodes Ready with correct roles
kubectl get nodes

# Calico running on every node (one pod per node)
kubectl get pods -n kube-system -o wide | grep calico-node

# CoreDNS running (cluster DNS)
kubectl get pods -n kube-system | grep coredns

# ArgoCD all pods Running
kubectl get pods -n argocd

# ArgoCD application Synced and Healthy
kubectl get applications -n argocd
```

Expected `kubectl get nodes` output:

```
NAME          STATUS   ROLES                     VERSION
edge-node     Ready    edge,worker               v1.31.14
master        Ready    control-plane             v1.31.14
tool-node-1   Ready    data-engineering,worker   v1.31.14
worker-1      Ready    vnf                       v1.31.14
worker-2      Ready    vnf                       v1.31.14
worker-3      Ready    vnf                       v1.31.14
worker-4      Ready    vnf                       v1.31.14
worker-5      Ready    vnf                       v1.31.14
worker-6      Ready    vnf                       v1.31.14
```