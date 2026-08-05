# Plan: Get a real 3-member Aeron Cluster running & verified on local Kubernetes

## Context

Milestone 9 (`docs/AERON-SEQUENCER-IMPLEMENTATION-STEPS.md`) is scaffolding-complete but not
"green": its 5 integration-tests `*IT` classes are `@Disabled` because the multi-member Raft
path they exercise has never actually run, and `cluster-node` genuinely has no code path to form
a multi-member cluster in Kubernetes — `ClusterNodeLauncher.main()` unconditionally builds a
single-member `ClusterNodeConfig` (`ClusterNodeConfig.singleMember`), and `50-failover-drill.sh`
is a stub that exits 1 with a "not implemented" message.

Rather than trying to close all of Milestone 9 at once, this plan scopes the **prerequisite**:
get `cluster-node` actually running as a real 3-pod Raft cluster on the local Docker Desktop
Kubernetes cluster, in namespace `gcm-md-local`, and verify it's genuinely forming
quorum/electing a leader/surviving a real pod kill — the same bar of live evidence CLAUDE.md
already has for the single-member deployment. **Multi-AZ measurement is explicitly deferred** —
this is single Docker-Desktop-node, 3 pods, no real cross-AZ network cost involved. Once this
lands, Milestone 9's `*IT` suite and the K8s-only pieces of 12.5 have something real to run
against.

The in-process 3-member harness (`InProcessCluster` / `ClusterNodeConfig.localMultiMember`,
used by `integration-tests`) is unaffected by this plan — it already works standalone and needs
no Kubernetes changes.

## What's grounded (verified by direct reads, not assumed)

- `ClusterNodeConfig` (`services/sequencer-aeron/cluster-node/.../ClusterNodeConfig.java`) is a
  record with 13 fields; 3 factories exist (`localSingleMember`, `singleMember(host, dataDir,
  sources)`, `localMultiMember(memberCount, baseDataDir, sources)`). No Kubernetes/N-member
  factory exists. `ingressChannel`/`logChannel` **must** stay endpoint-less templates
  (`"aeron:udp?term-length=64k"`) — a real bug already hit and fixed for single-member.
- `ClusterNodeLauncher.main()` (line 58) reads `CLUSTER_NODE_HOST`, `CLUSTER_DATA_DIR`,
  `CLUSTER_SOURCES`, `CLUSTER_METRICS_PORT` and unconditionally calls `singleMember(...)` at
  line 65 — no member-count/member-id env var exists.
- Helm StatefulSet (`deploy/helm/gcm-md-sequencer-aeron/templates/clusternode/statefulset.yaml`):
  `replicas` already comes from values (line 11), pods already get per-pod `POD_NAME` (downward
  API, lines 60-63) and a per-pod DNS `CLUSTER_NODE_HOST` (line 67). No `CLUSTER_MEMBER_ID` or
  peer-list env var exists. `persistence.enabled` (default `false`, `emptyDir`) is already a
  real toggle in the chart (lines 91-107) — just never turned on.
- `values.yaml` (chart defaults, line 7): `replicas: 1 # local default; AWS envs set 3` — the
  gap is called out explicitly in a comment (lines 12-15). No MTU field exists anywhere in the
  chart despite the design doc requiring it for Docker Desktop (default bridge MTU 1408).
- Namespace `market-data-local` is hardcoded independently in `20-install-nats.sh:6`,
  `30-deploy.sh:6`, `40-smoke-test.sh:13`, `90-teardown.sh:5`, plus baked into NATS URL defaults
  in `environments/local/values.yaml:38` and the chart's `values.yaml:100`. Also appears in prose
  in both design docs (`AERON-SEQUENCER-DESIGN.md:378`, `AERON-SEQUENCER-IMPLEMENTATION-STEPS.md:274`).
- `50-failover-drill.sh` is currently 16 lines, all stub: prints an error and `exit 1`
  unconditionally.
- `40-smoke-test.sh` runs `sequencer-loadgen` as a one-shot Job and reports throughput/latency,
  but by its own comment (lines 5-9) does **not** assert contiguous sequenceIds / duplicate
  absorption / heartbeat high-water-mark yet.
- `_helpers.tpl` has simple name helpers only (`gcm-md.fullname`, `gcm-md.clusterNode.name`,
  etc.) — no membership-list templating helper exists yet.

## Implementation steps

### 1. Namespace rename: `market-data-local` → `gcm-md-local`
Straight literal-string substitution, no parameterization needed, in:
`deploy/local/20-install-nats.sh` (line 6 + comment), `30-deploy.sh` (line 6),
`40-smoke-test.sh` (line 13), `90-teardown.sh` (line 5), `environments/local/values.yaml`
(line 38), `deploy/helm/gcm-md-sequencer-aeron/values.yaml` (line 100), plus the two prose
mentions in the design docs.
**Check:** `grep -rn "market-data-local" .` returns nothing.

### 2. `ClusterNodeConfig.kubernetesMember(...)` — new, additive factory
Add a 4th factory generalizing `singleMember`'s body to an externally-supplied
`clusterMemberId` and full `clusterMembers` string (same fixed port scheme: 9010/9020/9030/
9040/9050/9051/9060; same endpoint-less ingress/log templates). Zero changes to
`localSingleMember`/`singleMember`/`localMultiMember` — this must be strictly additive so the
existing, already-verified single-member path is untouched.
**Check:** new unit test in `ClusterNodeConfigTest` covering the membership-string format and
endpoint-less-template invariant; `mvn test` green; diff shows the three existing factories
byte-identical.

### 3. `ClusterNodeLauncher.main()` — branch on a new `CLUSTER_MEMBERS` env var
If `CLUSTER_MEMBERS` is set (non-blank), parse this pod's ordinal from `POD_NAME`'s
`<name>-<ordinal>` suffix (standard StatefulSet pattern; fail fast with a clear exception on a
malformed/missing `POD_NAME` — two pods silently claiming member 0 is worse than crashing) and
call `ClusterNodeConfig.kubernetesMember(ordinal, clusterMembersEnv, host, dataDir, sources)`.
Otherwise, fall through to today's unchanged `singleMember(...)` call — replicas=1 stays
byte-identical to today.
**Check:** unit test for the ordinal-parsing helper (valid `sts-0/1/2`, malformed input throws);
`mvn test` green.

### 4. MTU wiring (currently entirely absent, needed for reliable Docker Desktop UDP)
Add an `int mtuLength` overload of `launchNonBlocking` that sets `MediaDriver.Context
.mtuLength(...)`; existing 4-arg overload delegates with a documented default so
`InProcessCluster` and `launch()` need zero changes. `main()` reads a new `CLUSTER_MTU_LENGTH`
env var (default 1408). Add `clusterNode.aeron.mtuLength: 1408` to both `values.yaml` files;
render it as a StatefulSet env var (always, not gated).
**Check:** `mvn test` green; no changes needed at any existing call site of `launchNonBlocking`.

### 5. Helm: render `CLUSTER_MEMBERS`, wire `LOCAL_MEMBERS`, enable persistence for 3-member
- Add a `gcm-md.clusterNode.membersList` helper in `_helpers.tpl`: loop
  `range $i := until (int .Values.clusterNode.replicas)`, building the pipe-joined
  `memberId,client:port,...,archive:port` string from each pod's DNS name
  (`<sts>-<i>.<svc>.<namespace>.svc.cluster.local`) and `.Values.clusterNode.ports.*` — same
  format `ClusterNodeConfig` already documents.
- In `statefulset.yaml`, render `CLUSTER_MEMBERS` **only when `replicas > 1`** (so the rendered
  single-member pod spec is unchanged) and always render `CLUSTER_MTU_LENGTH`.
- `30-deploy.sh`: read a `LOCAL_MEMBERS` env var (default `1`), pass
  `--set clusterNode.replicas="$LOCAL_MEMBERS"` to `helm upgrade --install`.
- For the 3-member case, also pass `--set clusterNode.persistence.enabled=true` — with the
  default `emptyDir`, a `kubectl delete pod` wipes the killed member's data entirely rather than
  exercising real PVC-backed Raft catch-up.
**Check:** `helm lint deploy/helm/gcm-md-sequencer-aeron`; `helm template ... --set
clusterNode.replicas=3` — manually inspect the rendered `CLUSTER_MEMBERS` string (exactly 3
pipe-joined entries, correct ordinals/ports) and confirm it's **absent** when rendered with
`replicas=1` (regression check).

### 6. Regression check: redeploy single-member, confirm unchanged
`make local-up` with default `replicas: 1` — confirm still exactly today's behavior
(`ClusterTool is-leader` exit 0, one ACTIVE member) before touching multi-member.

### 7. Deploy 3-member to Docker Desktop, namespace `gcm-md-local`
`LOCAL_MEMBERS=3 deploy/local/30-deploy.sh`.
**Check:** `kubectl get pods -n gcm-md-local` shows 3 Running pods; `ClusterTool list-members`
(same invocation CLAUDE.md already confirms works inside a pod today) run against each of the 3
pods shows all 3 members in the same term with exactly one `isLeader=true`; `ClusterTool
describe`/`errors` show nothing. Re-run `40-smoke-test.sh` against the 3-member deployment to
confirm the ingress-shim → cluster → nats-bridge pipeline still flows messages end-to-end.

### 8. Failover drill — real content for `50-failover-drill.sh`
1. Preflight: confirm `clusterNode.replicas == 3` via `kubectl get statefulset -o
   jsonpath='{.spec.replicas}'`; fail with an actionable message otherwise.
2. Launch the loadgen Job (same pattern as `40-smoke-test.sh`), long enough to straddle the
   kill+re-election window.
3. Identify the current leader via `ClusterTool list-members` looped over the 3 pods.
4. `kubectl delete pod $leaderPod` mid-loadgen — a real ungraceful death, unlike
   `ClusterMemberHandle.close()`'s graceful shutdown (that distinction is already called out in
   `ClusterNodeLauncher`'s own Javadoc as this script's job).
5. Poll the two survivors until exactly one reports `isLeader=true` again (timeout ~60s).
6. Wait for the loadgen Job; dump logs; report pass/fail.
Minimal sequenceId-contiguity/duplicate check: `sequencer-loadgen`'s `LoadGenerator` already
decodes `sequenceId` per observed message (`recordOne`) but doesn't check for gaps/dupes yet —
add a small additive `--verify=true` flag (default off, existing behavior unchanged) tracking
min/max/count (contiguity) and a dedup set, non-zero exit on violation. Wire `--verify=true`
into both `40-smoke-test.sh` and the new drill script.
**Check:** full dry run — real leader pod killed mid-load, re-election completes within the poll
timeout, verifier reports zero gaps/duplicates across the failover boundary. Re-run 2-3 times —
Raft election timing is nondeterministic, so a single green run is weak evidence.

## Explicitly deferred (not in this plan)

- Multi-AZ / real inter-AZ RTT measurement (12.5) — needs a real multi-AZ cluster, out of scope
  for a single Docker Desktop node.
- Un-disabling/running the in-process `*IT` suite (12.2) and fixing the ArchUnit/Java 25
  incompatibility (12.3) — separate, unrelated pieces of closing Milestone 9, not prerequisites
  for this plan and not blocked by it either.
- Aeron driver name re-resolution behavior for rescheduled pods (design §5.2 calls for it;
  nothing configures it today) — flagged as a risk below, worth a small follow-up investigation
  but not required to get the first 3-member cluster running.

## Risk points (given CLAUDE.md's track record — 11 live-only bugs surfaced getting *single*-member working)

- **`CLUSTER_MEMBERS` string consistency across 3 independently-rendered env blocks** — verify
  the exact rendered string via `helm template` before ever deploying it; YAML block-scalar
  whitespace bugs are an easy way to get 3 pods parsing 3 subtly different strings.
- **`podManagementPolicy: Parallel`** — all 3 pods start simultaneously with no readiness gating
  between them, and the headless Service already sets `publishNotReadyAddresses: true`. Probably
  fine for Raft, but different timing than the fully-controlled in-process harness.
- **Driver name re-resolution** — nothing in `ClusterNodeLauncher` configures Aeron's
  re-resolution behavior; unverified whether a rescheduled pod (new IP, same DNS) reconnects
  without a full restart. Worth confirming against Aeron 1.46.5's actual defaults during step 7/8.
- **`ClusterTool` invocation inside the jib-built image** — confirm the exact classpath jib
  produces before relying on it for leader-detection in the drill script.
- **MTU context default vs. the hardcoded `term-length=64k` already in the channel URIs** —
  confirm during step 7 that the driver-level default actually takes effect and isn't silently
  overridden, given CLAUDE.md already documents one bug in this exact family (endpoint-less
  channel templates).

## Verification summary

End-to-end: `make local-up` (regression, replicas=1) → `LOCAL_MEMBERS=3 30-deploy.sh` → 3 pods
Running, `ClusterTool list-members` shows quorum + 1 leader across all 3 → `40-smoke-test.sh`
(with `--verify=true`) passes against the 3-member deployment → `50-failover-drill.sh` kills the
real leader pod and the verifier reports zero gaps/duplicates after re-election, repeated 2-3x
for confidence given Raft's nondeterministic timing.

## Status as of this session (live Docker Desktop run in `gcm-md-local`)

**Done and verified live:**
- Namespace renamed `market-data-local` → `gcm-md-local` everywhere (scripts, chart values,
  docs).
- `ClusterNodeConfig.kubernetesMember`, `ClusterNodeLauncher`'s `CLUSTER_MEMBERS`/pod-ordinal
  branch, MTU wiring (`CLUSTER_MTU_LENGTH`, default 1408), and the Helm `CLUSTER_MEMBERS`/
  `ingressEndpoints` template helpers are implemented and unit-tested (`mvn test` green
  reactor-wide).
- **A real 3-member Aeron/Raft cluster is running and verified live**: `kubectl get pods` shows
  3/3 cluster-node pods Running; `ClusterTool list-members`/`is-leader` on all 3 pods agree on
  exactly one leader, zero consensus-module errors; a leadership change was observed for real
  across a rolling restart (2 → 1 → 0 over the session). This is the core deliverable and it
  works.
- Two real, previously-latent bugs found and fixed along the way: (1) `LoadGenerator.parseArgs`
  only accepts space-separated `--flag value` pairs, not `--flag=value` — both `40-smoke-test.sh`
  and `50-failover-drill.sh` were using the wrong format (never actually run before, per
  CLAUDE.md). (2) `ingress-shim`'s `INGRESS_SHIM_CLUSTER_INGRESS_ENDPOINTS` was hardcoded to the
  bare headless-Service DNS name (`0=<svc-name>:9010`) instead of a real per-member endpoint
  list — fixed via a new `gcm-md.clusterNode.ingressEndpoints` Helm helper. This was silently
  "working" for replicas=1 only because the bare service name happens to resolve to exactly one
  IP with a single pod.
- Local resource limits tuned: chart defaults (1 CPU / 2Gi per cluster-node pod × 3, plus
  ingress-shim/nats-bridge/NATS) exceed Docker Desktop's ~7Gi allocation; `environments/local/
  values.yaml` now requests less memory but keeps CPU limits generous (confirmed via cgroup
  `cpu.stat` that a too-tight CPU limit causes real throttling, which is a bigger risk to a
  Raft session-connect timeout than memory pressure on this machine).

**Not resolved — open bug, root cause not found:** `ingress-shim` cannot complete its
`AeronCluster` session-connect handshake to the cluster (`io.aeron.exceptions.TimeoutException`
at `ClusterIngressClient.<init>`), even after fixing the endpoint-list bug above. Extensive live
diagnosis this session:
- Ruled out: NetworkPolicy (only restricts cluster-node's own ingress; nothing restricts
  ingress-shim), DNS resolution (confirmed correct and fresh via `getent hosts` matching actual
  pod IPs), CPU throttling as the *sole* cause (raised limits, reproduces identically even fully
  unconstrained in an ad-hoc debug pod with no resource limits at all), raw pod-to-pod UDP
  reachability in general (proven bidirectional with a `nc`-based test between scratch pods),
  `/dev/shm` writability for the embedded media driver (confirmed writable, 64Mi default).
- Confirmed via `AeronStat` against the live CnC file: cluster-node's own driver shows healthy,
  substantial traffic (millions of bytes sent/received, real heartbeats/status messages) and IS
  listening on port 9010 (`rcv-channel ... :9010`). ingress-shim's own embedded driver, by
  contrast, shows **zero bytes sent** system-wide even after a 120-second connect timeout
  (`AeronCluster.Context.messageTimeoutNs` overridden via
  `-Daeron.cluster.message.timeout` to confirm it isn't just "needs more time" — it doesn't
  help), despite having registered publications toward all 3 members. The failure state is
  `AWAIT_PUBLICATION_CONNECTED` with `ingressPublication=null` — none of the 3 per-member
  ingress publications ever report connected from the client's own polling.
- This reproduces identically both in the real Helm-deployed pod and in a from-scratch ad-hoc
  debug pod running the same jar manually with the same env vars, which rules out anything
  specific to the Helm chart's pod spec (securityContext, resource limits, `readOnlyRootFilesystem`).
- Not yet tried: resetting Docker Desktop's Kubernetes cluster to rule out CNI/conntrack state
  accumulated from this session's heavy churn (many `kubectl debug` ephemeral containers, pod
  deletes, repeated helm upgrades); enabling Aeron's own event-logging agent for byte-level
  packet tracing (needs `-javaagent`, not attempted this session).

**Recommendation:** treat this as a separate, scoped follow-up investigation — it blocks
`40-smoke-test.sh`'s pipeline check and `50-failover-drill.sh` (both need a working ingress
path), but does not affect the core 3-member Raft verification above, which is complete and
solid.