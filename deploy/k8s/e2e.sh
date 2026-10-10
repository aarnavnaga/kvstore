#!/usr/bin/env bash
# End-to-end test on a local Kubernetes cluster (kind), driven from a tools container.
#
#   deploy/k8s/e2e.sh          create the cluster if needed, install, test
#   deploy/k8s/e2e.sh down     delete the kind cluster
#
# Checks: helm install brings up a healthy 3-node cluster with mTLS on; reads and writes
# work with the cluster certificate and are refused without one; the cluster survives
# losing its leader pod; Prometheus scrapes every node; a helm upgrade keeps the CA.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
docker build -q -t kvstore-k8s-tools -f "$ROOT/deploy/k8s/Dockerfile.tools" "$ROOT/deploy/k8s" >/dev/null
exec docker run --rm -i -v /var/run/docker.sock:/var/run/docker.sock -v "$ROOT:/work" \
  --network host -e MODE="${1:-up}" kvstore-k8s-tools bash /work/deploy/k8s/e2e-inside.sh
