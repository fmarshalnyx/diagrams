#!/usr/bin/env bash
# Design §11: deploy infra/nats-setup into gcm-md-local and run its stream/KV setup Job.
set -euo pipefail
cd "$(dirname "$0")/../.."

NAMESPACE="gcm-md-local"

kubectl get namespace "$NAMESPACE" >/dev/null 2>&1 || kubectl create namespace "$NAMESPACE"

helm dependency build infra/nats-setup

helm upgrade --install nats-setup infra/nats-setup \
  --namespace "$NAMESPACE" \
  --wait --timeout 3m

echo "Waiting for NATS StatefulSet to be ready..."
kubectl rollout status statefulset/nats --namespace "$NAMESPACE" --timeout=3m

# The setup Job's name is fixed (not per-release), so a re-run needs the prior completed Job
# deleted first — Jobs are immutable once created (idempotent overall, not in-place).
if kubectl get job nats-setup --namespace "$NAMESPACE" >/dev/null 2>&1; then
  kubectl delete job nats-setup --namespace "$NAMESPACE" --wait
fi
helm upgrade --install nats-setup infra/nats-setup --namespace "$NAMESPACE"
kubectl wait --for=condition=complete job/nats-setup --namespace "$NAMESPACE" --timeout=3m
kubectl logs job/nats-setup --namespace "$NAMESPACE"

echo "NATS + stream/KV setup complete."
