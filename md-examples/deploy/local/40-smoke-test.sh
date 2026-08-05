#!/usr/bin/env bash
# Design §11 (revised, Milestone 5B.3): confirm the mock-upstream-source -> line-handler-template
# -> cluster -> nats-bridge -> md.sequenced -> mock-upstream-source loop is healthy, using the
# persistent 5B services (sequencer-loadgen, the one-shot Job this replaced, was retired in
# Milestone 5B.4).
#
# Unlike loadgen, mock-upstream-source is an always-on Deployment (deployed by 30-deploy.sh, not
# launched here) that generates traffic and verifies contiguity/no-duplicates on the observed
# egress continuously — this script just samples its Prometheus counters before and after a
# window and reports throughput plus any *new* violations, rather than launching a burst itself.
#
# Baseline gap/duplicate counts are deliberately not asserted at zero outright: they're a running
# total since the pod last started, not a "since last check" count, and a run right after a real
# incident (see implementation-steps.md's "New findings" for one such incident, since fixed) would
# otherwise never let this script pass no matter how healthy things currently are. This script's
# job is to catch a regression introduced during the window it observes.
set -euo pipefail
cd "$(dirname "$0")/../.."

NAMESPACE="gcm-md-local"
DURATION="${1:-30}"

MOCK_DEPLOY="deploy/gcm-md-sequencer-aeron-mock-upstream-source"
LINE_HANDLER_DEPLOY="deploy/gcm-md-sequencer-aeron-line-handler-template"

scrape() {
  local deploy="$1" metric="$2"
  kubectl exec "$deploy" --namespace "$NAMESPACE" -- wget -qO- http://localhost:8080/actuator/prometheus 2>/dev/null \
    | awk -v m="$metric" '$0 ~ "^" m "\\{" { print $2 }'
}

echo "Preflight: confirming mock-upstream-source and line-handler-template are ready..."
kubectl rollout status "$MOCK_DEPLOY" --namespace "$NAMESPACE" --timeout=60s
kubectl rollout status "$LINE_HANDLER_DEPLOY" --namespace "$NAMESPACE" --timeout=60s

echo "Sampling baseline metrics..."
gap_before="$(scrape "$MOCK_DEPLOY" mock_upstream_gap)"
dup_before="$(scrape "$MOCK_DEPLOY" mock_upstream_duplicate)"
observed_before="$(scrape "$MOCK_DEPLOY" mock_upstream_observed)"
relayed_before="$(scrape "$LINE_HANDLER_DEPLOY" line_handler_messages_relayed)"
relay_errors_before="$(scrape "$LINE_HANDLER_DEPLOY" line_handler_relay_loop_errors)"
echo "Baseline: gap=$gap_before duplicate=$dup_before observed=$observed_before relayed=$relayed_before relay_errors=$relay_errors_before"

echo "Observing for ${DURATION}s..."
sleep "$DURATION"

gap_after="$(scrape "$MOCK_DEPLOY" mock_upstream_gap)"
dup_after="$(scrape "$MOCK_DEPLOY" mock_upstream_duplicate)"
observed_after="$(scrape "$MOCK_DEPLOY" mock_upstream_observed)"
relayed_after="$(scrape "$LINE_HANDLER_DEPLOY" line_handler_messages_relayed)"
relay_errors_after="$(scrape "$LINE_HANDLER_DEPLOY" line_handler_relay_loop_errors)"

observed_delta=$(awk -v a="$observed_before" -v b="$observed_after" 'BEGIN{printf "%.0f", b-a}')
relayed_delta=$(awk -v a="$relayed_before" -v b="$relayed_after" 'BEGIN{printf "%.0f", b-a}')
gap_delta=$(awk -v a="$gap_before" -v b="$gap_after" 'BEGIN{printf "%.0f", b-a}')
dup_delta=$(awk -v a="$dup_before" -v b="$dup_after" 'BEGIN{printf "%.0f", b-a}')
relay_errors_delta=$(awk -v a="$relay_errors_before" -v b="$relay_errors_after" 'BEGIN{printf "%.0f", b-a}')

echo "After ${DURATION}s: observed +$observed_delta ($(( observed_delta / DURATION )) msg/s), relayed +$relayed_delta ($(( relayed_delta / DURATION )) msg/s)"
echo "New gaps: $gap_delta, new duplicates: $dup_delta, new relay loop errors: $relay_errors_delta"

fail=0
if [[ "$observed_delta" -le 0 ]]; then
  echo "FAIL: no new messages observed on egress during the window — pipeline is stalled." >&2
  fail=1
fi
if [[ "$relayed_delta" -le 0 ]]; then
  echo "FAIL: line-handler-template relayed no messages during the window." >&2
  fail=1
fi
if [[ "$gap_delta" -gt 0 ]]; then
  echo "FAIL: $gap_delta new gap(s) appeared on the observed egress during the window." >&2
  fail=1
fi
if [[ "$dup_delta" -gt 0 ]]; then
  echo "FAIL: $dup_delta new duplicate(s) appeared on the observed egress during the window." >&2
  fail=1
fi
if [[ "$relay_errors_delta" -gt 0 ]]; then
  echo "FAIL: $relay_errors_delta new line-handler-template relay loop error(s) during the window." >&2
  fail=1
fi

if [[ "$fail" -ne 0 ]]; then
  exit 1
fi

echo "Smoke test PASSED."
