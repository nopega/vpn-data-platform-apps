#!/usr/bin/env bash
#
# images/spark/build.sh [tag] -- build the Spark job image and push it to Harbor.
#
#   harbor.nopega.net/ice-berg-platform/datapipeline:<tag>   (default tag 0.1.0)
#
# --platform linux/amd64: the Mac is ARM, the nodes are x86. An ARM image
# pulls fine and then every pod dies with "exec format error". Building for
# amd64 on an ARM Mac runs under emulation: expect 10-20 minutes.
#
# Needs: Docker Desktop running, Tailscale connected, and
#   docker login harbor.nopega.net   (VPN_DATA_PLATFORM: ./bootstrap/harbor.sh show)

set -euo pipefail

TAG="${1:-0.1.0}"
IMAGE="harbor.nopega.net/ice-berg-platform/datapipeline:${TAG}"

cd "$(dirname "$0")"
docker buildx build --platform linux/amd64 -t "$IMAGE" --push .

echo ""
echo "pushed $IMAGE"
echo "SparkApplications reference this tag (runs/smoke-test.yaml)."
