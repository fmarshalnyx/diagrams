#!/usr/bin/env bash
# Design §11: helm upgrade --install the aeron chart with environments/local/ values.
#
# LOCAL_MEMBERS (default 1) selects the cluster-node profile: 1 for normal local dev, 3 for the
# failover-drill profile (50-failover-drill.sh). >1 also enables PVC-backed persistence, so a
# killed member's data survives pod recreation and exercises real Raft catch-up instead of a
# from-empty rejoin (see docs/AERON-SEQUENCER-3-MEMBER-CLUSTER-PLAN.md).
set -euo pipefail
cd "$(dirname "$0")/../.."

NAMESPACE="gcm-md-local"
LOCAL_MEMBERS="${LOCAL_MEMBERS:-1}"

HELM_ARGS=(--set "clusterNode.replicas=$LOCAL_MEMBERS")
if [[ "$LOCAL_MEMBERS" -gt 1 ]]; then
  HELM_ARGS+=(--set "clusterNode.persistence.enabled=true")
fi

helm upgrade --install gcm-md-sequencer-aeron deploy/helm/gcm-md-sequencer-aeron \
  --namespace "$NAMESPACE" \
  -f environments/local/values.yaml \
  "${HELM_ARGS[@]}" \
  --wait --timeout 5m

kubectl get pods --namespace "$NAMESPACE" -l app.kubernetes.io/part-of=gcm-md-sequencer-aeron
