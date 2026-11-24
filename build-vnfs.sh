#!/bin/bash
# Build and push all VNF Docker images
# Run from: ~/Downloads/Projects/iot-vnf-platform/
#
# Prerequisites:
#   docker buildx installed
#   logged into ghcr.io:
#     echo <GITHUB_PAT> | docker login ghcr.io -u mld-rafiee --password-stdin

set -e

REGISTRY="ghcr.io/mld-rafiee/iot-vnf-platform"
PLATFORM="linux/amd64"

echo "================================================"
echo "Building VNF Docker images for linux/amd64"
echo "Registry: $REGISTRY"
echo "================================================"

# ── Firewall (used for both FW1 and FW2) ─────────────────────────────────────
echo ""
echo "[1/5] Building firewall..."
docker buildx build \
    --platform $PLATFORM \
    --file vnfs/firewall/Dockerfile \
    -t $REGISTRY/firewall:latest \
    --push \
    .
echo "✓ firewall pushed"

# ── DPI ───────────────────────────────────────────────────────────────────────
echo ""
echo "[2/5] Building dpi..."
docker buildx build \
    --platform $PLATFORM \
    --file vnfs/dpi/Dockerfile \
    -t $REGISTRY/dpi:latest \
    --push \
    .
echo "✓ dpi pushed"

# ── Encryption ────────────────────────────────────────────────────────────────
echo ""
echo "[3/5] Building encryption..."
docker buildx build \
    --platform $PLATFORM \
    --file vnfs/encryption/Dockerfile \
    -t $REGISTRY/encryption:latest \
    --push \
    .
echo "✓ encryption pushed"

# ── Compression ───────────────────────────────────────────────────────────────
echo ""
echo "[4/5] Building compression..."
docker buildx build \
    --platform $PLATFORM \
    --file vnfs/compression/Dockerfile \
    -t $REGISTRY/compression:latest \
    --push \
    .
echo "✓ compression pushed"

# ── NAT ───────────────────────────────────────────────────────────────────────
echo ""
echo "[5/5] Building nat..."
docker buildx build \
    --platform $PLATFORM \
    --file vnfs/nat/Dockerfile \
    -t $REGISTRY/nat:latest \
    --push \
    .
echo "✓ nat pushed"

echo ""
echo "================================================"
echo "All VNF images built and pushed successfully"
echo "================================================"
echo ""
echo "Images:"
echo "  $REGISTRY/firewall:latest"
echo "  $REGISTRY/dpi:latest"
echo "  $REGISTRY/encryption:latest"
echo "  $REGISTRY/compression:latest"
echo "  $REGISTRY/nat:latest"
