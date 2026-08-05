#!/usr/bin/env bash
# Design §11 (3-member mode, revised for Milestone 5B.3): kubectl delete pod the leader while
# mock-upstream-source's always-on traffic keeps flowing, then confirm its own contiguity/
# no-duplicate verifier saw no new violations across the kill+re-election window (§6.4 as a
# laptop drill). Real ungraceful kubectl delete pod (not ClusterMemberHandle.close()'s graceful
# shutdown) — see ClusterNodeLauncher's own Javadoc on that distinction.
#
# Replaces the old sequencer-loadgen-based drill (loadgen was retired in Milestone 5B.4):
# mock-upstream-source's EgressConsumer already
# subscribes to md.sequenced continuously and tracks gap/duplicate counts, so this drill reads
# those counters before/after instead of launching and waiting on a one-shot Job. Requires the 3-
# member cluster-node profile: run `LOCAL_MEMBERS=3 deploy/local/30-deploy.sh` first (see
# docs/AERON-SEQUENCER-3-MEMBER-CLUSTER-PLAN.md).
set -euo pipefail
cd "$(dirname "$0")/../.."

NAMESPACE="gcm-md-local"
STATEFULSET="gcm-md-sequencer-aeron-cluster-node"
CLASSPATH='/app/resources:/app/classes:/app/libs/*'
DURATION="${1:-60}"
ELECTION_TIMEOUT_SECONDS=60

MOCK_DEPLOY="deploy/gcm-md-sequencer-aeron-mock-upstream-source"

replicas="$(kubectl get statefulset "$STATEFULSET" --namespace "$NAMESPACE" -o jsonpath='{.spec.replicas}' 2>/dev/null || echo 0)"
if [[ "$replicas" -lt 3 ]]; then
  echo "ERROR: $STATEFULSET has $replicas replica(s), need >= 3 for a failover drill." >&2
  echo "Run: LOCAL_MEMBERS=3 deploy/local/30-deploy.sh" >&2
  exit 1
fi

cluster_tool() {
  local pod="$1"; shift
  kubectl exec "$pod" --namespace "$NAMESPACE" -- java -cp "$CLASSPATH" io.aeron.cluster.ClusterTool /data/cluster "$@" 2>/dev/null
}

find_leader() {
  local i output
  for (( i = 0; i < replicas; i++ )); do
    local pod="${STATEFULSET}-${i}"
    output="$(cluster_tool "$pod" list-members || true)"
    if [[ "$output" == *"isLeader=true"* ]]; then
      echo "$pod"
      return 0
    fi
  done
  return 1
}

scrape() {
  local metric="$1"
  kubectl exec "$MOCK_DEPLOY" --namespace "$NAMESPACE" -- wget -qO- http://localhost:8080/actuator/prometheus 2>/dev/null \
    | awk -v m="$metric" '$0 ~ "^" m "\\{" { print $2 }'
}

echo "Preflight: confirming mock-upstream-source is ready and locating current leader among $replicas members..."
kubectl rollout status "$MOCK_DEPLOY" --namespace "$NAMESPACE" --timeout=60s
leader_pod="$(find_leader)" || { echo "ERROR: no member currently reports isLeader=true." >&2; exit 1; }
echo "Current leader: $leader_pod"

gap_before="$(scrape mock_upstream_gap)"
dup_before="$(scrape mock_upstream_duplicate)"
observed_before="$(scrape mock_upstream_observed)"
echo "Baseline: gap=$gap_before duplicate=$dup_before observed=$observed_before"

# Straddle the kill mid-drill rather than at the very start or end.
sleep "$(( DURATION / 3 ))"

echo "Killing leader pod $leader_pod (real ungraceful kubectl delete, not a graceful stop)..."
kubectl delete pod "$leader_pod" --namespace "$NAMESPACE" --grace-period=0 --force

echo "Waiting for re-election among the survivors (timeout ${ELECTION_TIMEOUT_SECONDS}s)..."
elapsed=0
new_leader=""
while (( elapsed < ELECTION_TIMEOUT_SECONDS )); do
  if new_leader="$(find_leader)" && [[ "$new_leader" != "$leader_pod" ]]; then
    break
  fi
  new_leader=""
  sleep 2
  elapsed=$(( elapsed + 2 ))
done

if [[ -z "$new_leader" ]]; then
  echo "ERROR: no new leader elected within ${ELECTION_TIMEOUT_SECONDS}s." >&2
  exit 1
fi
echo "Re-election complete after ~${elapsed}s: new leader is $new_leader"

remaining=$(( DURATION - DURATION / 3 - elapsed ))
if [[ "$remaining" -gt 0 ]]; then
  echo "Continuing to observe for ${remaining}s after re-election..."
  sleep "$remaining"
fi

gap_after="$(scrape mock_upstream_gap)"
dup_after="$(scrape mock_upstream_duplicate)"
observed_after="$(scrape mock_upstream_observed)"

gap_delta=$(awk -v a="$gap_before" -v b="$gap_after" 'BEGIN{printf "%.0f", b-a}')
dup_delta=$(awk -v a="$dup_before" -v b="$dup_after" 'BEGIN{printf "%.0f", b-a}')
observed_delta=$(awk -v a="$observed_before" -v b="$observed_after" 'BEGIN{printf "%.0f", b-a}')

echo "Across the drill: observed +$observed_delta, new gaps: $gap_delta, new duplicates: $dup_delta"

fail=0
if [[ "$observed_delta" -le 0 ]]; then
  echo "FAIL: no new messages observed across the drill — pipeline did not recover." >&2
  fail=1
fi
if [[ "$gap_delta" -gt 0 ]]; then
  echo "FAIL: $gap_delta new gap(s) appeared across the leader kill — failover is not gap-free." >&2
  fail=1
fi
if [[ "$dup_delta" -gt 0 ]]; then
  echo "FAIL: $dup_delta new duplicate(s) appeared across the leader kill — failover double-sequenced." >&2
  fail=1
fi

if [[ "$fail" -ne 0 ]]; then
  exit 1
fi

echo "Failover drill PASSED: leader killed, re-elected in ~${elapsed}s, zero new duplicates/gaps observed."
