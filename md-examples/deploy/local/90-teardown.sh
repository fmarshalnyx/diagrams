#!/usr/bin/env bash
# Design §11: helm uninstall all releases, delete namespace, optional PVC wipe.
set -euo pipefail

NAMESPACE="gcm-md-local"
WIPE_PVCS="${1:-}"

# helm uninstall has no --ignore-not-found flag (unlike kubectl) — || true swallows the
# "release not found" error instead when re-running teardown on an already-clean namespace.
helm uninstall gcm-md-sequencer-aeron --namespace "$NAMESPACE" || true
helm uninstall nats-setup --namespace "$NAMESPACE" || true

if [[ "$WIPE_PVCS" == "--wipe-pvcs" ]]; then
  kubectl delete namespace "$NAMESPACE" --ignore-not-found
else
  echo "Namespace $NAMESPACE left in place (PVCs preserved). Pass --wipe-pvcs to delete it entirely."
fi
