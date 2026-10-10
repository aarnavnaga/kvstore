#!/usr/bin/env bash
# Runs inside the tools container (see e2e.sh).
set -euo pipefail
CLUSTER=kvstore
NS=kvstore
REL=kvstore

if [ "${MODE:-up}" = "down" ]; then
  kind delete cluster --name "$CLUSTER"
  exit 0
fi

say() { echo "==> $*"; }
fail() { echo "FAIL: $*"; exit 1; }

if ! kind get clusters | grep -qx "$CLUSTER"; then
  say "creating kind cluster"
  kind create cluster --name "$CLUSTER" --config /work/deploy/k8s/kind.yaml --wait 120s
fi
kind export kubeconfig --name "$CLUSTER" >/dev/null

say "loading kvstore:dev into the cluster"
kind load docker-image kvstore:dev --name "$CLUSTER" >/dev/null

say "helm install"
kubectl create namespace "$NS" --dry-run=client -o yaml | kubectl apply -f - >/dev/null
helm upgrade --install "$REL" /work/deploy/helm/kvstore -n "$NS" --wait --timeout 5m
kubectl -n "$NS" rollout status statefulset/"$REL" --timeout=3m
kubectl -n "$NS" get pods -o wide

TLS="--tls_ca=/etc/kvstore/tls/ca.crt --tls_cert=/etc/kvstore/tls/tls.crt --tls_key=/etc/kvstore/tls/tls.key"
HOSTS=""
for i in 0 1 2; do HOSTS="$HOSTS,$REL-$i.$REL-peers.$NS.svc.cluster.local:7000"; done
SERVERS="--servers=${HOSTS#,}"
kv() { kubectl -n "$NS" exec "$1" -- kvcli $SERVERS $TLS --timeout_ms=10000 "${@:2}"; }

say "node status"
STATUS=$(kv "$REL-0" status)
echo "$STATUS"
[ "$(echo "$STATUS" | grep -c 'role=leader')" = "1" ] || fail "expected exactly one leader"
echo "$STATUS" | grep -q 'role=follower' || fail "no followers"

say "writes and reads over mutual TLS"
for i in $(seq 1 50); do kv "$REL-$((i % 3))" put "key$i" "value$i" >/dev/null; done
[ "$(kv "$REL-2" get key37)" = "value37" ] || fail "read after write"

say "a client without a certificate is refused"
if kubectl -n "$NS" exec "$REL-0" -- kvcli $SERVERS --timeout_ms=2000 get key1 >/dev/null 2>&1; then
  fail "plaintext client was served"
fi
echo "refused, as expected"

say "deleting the leader pod"
LEADER=$(echo "$STATUS" | awk '/role=leader/ { split($1, a, "."); print a[1] }')
kubectl -n "$NS" delete pod "$LEADER" --wait=false
sleep 2
for _ in $(seq 1 60); do
  S=$(kv "$REL-0" status 2>/dev/null || true)
  NEW=$(echo "$S" | awk '/role=leader/ { split($1, a, "."); print a[1] }')
  [ -n "$NEW" ] && [ "$NEW" != "$LEADER" ] && break
  sleep 1
done
echo "new leader: ${NEW:-none}"
[ -n "${NEW:-}" ] || fail "no new leader"
kv "$REL-1" put after failover >/dev/null || fail "write after failover"
kubectl -n "$NS" rollout status statefulset/"$REL" --timeout=3m
[ "$(kv "$LEADER" get key1)" = "value1" ] || fail "data on the recreated pod"

say "Prometheus scrapes every node"
for _ in $(seq 1 30); do
  UP=$(kubectl -n "$NS" exec deploy/"$REL"-prometheus -- \
    wget -qO- 'http://localhost:9090/api/v1/query?query=sum(up{job="kvstore"})' 2>/dev/null \
    | sed -E 's/.*"value":\[[^,]+,"([0-9.]+)".*/\1/' || true)
  [ "$UP" = "3" ] && break
  sleep 2
done
echo "targets up: ${UP:-0}"
[ "${UP:-0}" = "3" ] || fail "Prometheus does not see all three nodes"

say "rolling restart: every pod gets a new IP and must rejoin"
kubectl -n "$NS" rollout restart statefulset/"$REL"
kubectl -n "$NS" rollout status statefulset/"$REL" --timeout=3m
[ "$(kv "$REL-2" get key37)" = "value37" ] || fail "read after rolling restart"
S=$(kv "$REL-0" status)
[ "$(echo "$S" | grep -c 'role=leader')" = "1" ] || fail "no single leader after rolling restart"

say "helm upgrade keeps the cluster CA"
CA1=$(kubectl -n "$NS" get secret "$REL-tls" -o jsonpath='{.data.ca\.crt}' | md5sum)
helm upgrade "$REL" /work/deploy/helm/kvstore -n "$NS" --wait --timeout 5m >/dev/null
CA2=$(kubectl -n "$NS" get secret "$REL-tls" -o jsonpath='{.data.ca\.crt}' | md5sum)
[ "$CA1" = "$CA2" ] || fail "upgrade regenerated the CA"
[ "$(kv "$REL-0" get after)" = "failover" ] || fail "read after upgrade"

say "PASS: 3-node cluster healthy over mutual TLS on Kubernetes"
