#!/bin/bash
# K8s Node Preparation Script
# IoT VNF Platform — Phase 0
# Run this on every worker node

set -e  # stop if any command fails

echo "==> [1/6] Updating system packages..."
sudo apt update && sudo apt upgrade -y || true
sudo apt --fix-broken install -y
sudo apt upgrade -y

echo "==> [2/6] Disabling swap..."
sudo swapoff -a
sudo sed -i '/ swap / s/^\(.*\)$/#\1/g' /etc/fstab

echo "==> [3/6] Loading kernel modules..."
sudo modprobe overlay
sudo modprobe br_netfilter
cat <<MODULES | sudo tee /etc/modules-load.d/k8s.conf
overlay
br_netfilter
MODULES

echo "==> [4/6] Configuring kernel network settings..."
cat <<SYSCTL | sudo tee /etc/sysctl.d/k8s.conf
net.bridge.bridge-nf-call-iptables  = 1
net.bridge.bridge-nf-call-ip6tables = 1
net.ipv4.ip_forward                 = 1
SYSCTL
sudo sysctl --system

echo "==> [5/6] Installing containerd..."
sudo apt install -y containerd
sudo mkdir -p /etc/containerd
containerd config default | sudo tee /etc/containerd/config.toml
sudo sed -i 's/SystemdCgroup = false/SystemdCgroup = true/' /etc/containerd/config.toml
sudo systemctl restart containerd
sudo systemctl enable containerd

echo "==> [6/6] Installing kubeadm, kubelet, kubectl..."
sudo apt install -y apt-transport-https ca-certificates curl gpg
curl -fsSL https://pkgs.k8s.io/core:/stable:/v1.31/deb/Release.key | \
  sudo gpg --dearmor -o /etc/apt/keyrings/kubernetes-apt-keyring.gpg
echo 'deb [signed-by=/etc/apt/keyrings/kubernetes-apt-keyring.gpg] https://pkgs.k8s.io/core:/stable:/v1.31/deb/ /' | \
  sudo tee /etc/apt/sources.list.d/kubernetes.list
sudo apt update
sudo apt install -y kubelet kubeadm kubectl
sudo apt-mark hold kubelet kubeadm kubectl

echo ""
echo "✅ Node preparation complete!"
echo "   kubeadm: $(kubeadm version --output short)"
echo "   kubectl: $(kubectl version --client --output yaml | grep gitVersion | head -1)"
