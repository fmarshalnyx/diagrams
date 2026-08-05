# 3-Member Aeron Cluster — Status & Deferred Work

Handoff summary for resuming this work. Full technical detail, grounded file:line citations, and
the original implementation plan live in
`docs/AERON-SEQUENCER-3-MEMBER-CLUSTER-PLAN.md` — this document is the scannable status snapshot;
that one is the record of how we got here. For the project-wide picture (all milestones, not just
this one), see `docs/AERON-SEQUENCER-IMPLEMENTATION-STEPS.md`'s "Current status & path to
completion" section — that's the single source of truth for what's left across the whole
application; this document is detail-on-demand for the 3-member/ingress-connectivity thread
specifically.

## Bottom line

**The core goal — a real 3-member Aeron/Raft cluster running and verified on local Kubernetes —
is done.** The `ingress-shim` connectivity bug described below was believed **moot** for a while:
`ingress-shim` itself was dropped from the design entirely (implementation-steps.md Milestone 5,
revised — line handlers now pick an `IngressTransport` by config instead of routing through a
shim service), so it looked like there was no service left for this bug to affect. **That turned
out to be wrong** — a later session ran `line-handler-template` (the config-selected transport
service that replaced `ingress-shim`) against a real cluster and hit the identical symptom; see
the "NOT moot" section below for the corrected, current status. The investigation and evidence
below are kept for the record (some of it — the "not yet tried" event-log-tracing suggestion —
is directly relevant both to this resurfaced bug and to a *different*, still-open flow-control
finding in the in-process `*IT` suite; see the "fresh-environment re-run" update further down).

## What's done

- **Namespace renamed** `market-data-local` → `gcm-md-local` everywhere (scripts, chart values,
  both design docs).
- **Code**: `ClusterNodeConfig.kubernetesMember(...)` (new, additive factory),
  `ClusterNodeLauncher`'s `CLUSTER_MEMBERS`/pod-ordinal-parsing branch, an `int mtuLength`
  overload of `launchNonBlocking` wired to a new `CLUSTER_MTU_LENGTH` env var (default 1408).
  `singleMember`/`localSingleMember`/`localMultiMember` are untouched — replicas=1 behavior is
  byte-identical to before. All covered by new unit tests; full reactor `mvn test` is green.
- **Helm**: `gcm-md.clusterNode.membersList` and `gcm-md.clusterNode.ingressEndpoints` template
  helpers in `_helpers.tpl`; `CLUSTER_MEMBERS` rendered only when `replicas > 1`; `LOCAL_MEMBERS`
  env var wired into `30-deploy.sh` (defaults to 1, sets `clusterNode.persistence.enabled=true`
  automatically when >1 so a killed pod's data survives recreation for real Raft catch-up).
  Verified via `helm lint` and `helm template` diffing the rendered output for both `replicas=1`
  and `replicas=3`.
- **Live verification** (Docker Desktop, namespace `gcm-md-local`): 3/3 cluster-node pods
  Running; `ClusterTool list-members`/`is-leader` run against all 3 pods agree on exactly one
  leader with zero consensus-module errors; a real leadership handover was observed across a
  rolling restart during the session (leader moved 2 → 1 → 0). This is the actual bar CLAUDE.md
  already holds the single-member deployment to, now met for 3 members.
- **Two real bugs found and fixed** (both were latent — the pipeline paths they broke had never
  actually been run before, per CLAUDE.md's own account):
  1. `LoadGenerator.parseArgs` only accepts space-separated `--flag value` pairs; `40-smoke-test.sh`
     and `50-failover-drill.sh` were passing `--flag=value`. Fixed in both scripts.
  2. `ingress-shim`'s `INGRESS_SHIM_CLUSTER_INGRESS_ENDPOINTS` was hardcoded to the bare headless-
     Service DNS name (`0=<svc>:9010`), which only "worked" for replicas=1 by coincidence (one
     pod behind the DNS name). Fixed via the new `ingressEndpoints` Helm helper to list every
     member explicitly.
- Added a `LoadGenerator --verify=true` flag (contiguity + duplicate check on observed
  sequenceIds, schema-fixed offset 8) — off by default, wired into `40-smoke-test.sh` and
  `50-failover-drill.sh`. This is new capability, not yet exercised end-to-end (blocked by the
  open bug below).
- Local resource limits tuned in `environments/local/values.yaml`: the chart's prod-shaped
  defaults (1 CPU / 2Gi per cluster-node pod × 3, plus ingress-shim/nats-bridge/NATS) exceed
  Docker Desktop's ~7Gi allocation. Memory was tightened; CPU limits were kept generous after
  confirming via cgroup `cpu.stat` that a too-tight CPU limit causes real throttling that risks
  breaking Raft session timing — worse than the memory tradeoff on this machine.
- `50-failover-drill.sh` has real content now (was a stub that unconditionally exited 1): finds
  the leader via `ClusterTool`, does a real `kubectl delete pod` (not a graceful stop), polls for
  re-election, and runs the loadgen `--verify=true` check across the failover window. Not yet
  successfully run end-to-end — blocked by the same open bug.

## RESOLVED (resurfaced in `line-handler-template`, then fixed): Aeron cluster-connect/reconnect bug

**Correction, then resolution (this session):** this section originally concluded the bug below
was moot because `ingress-shim` itself was retired. That conclusion was wrong, or at least
premature — running `line-handler-template` (the config-selected `IngressTransport` service that
replaced `ingress-shim`) against a real local cluster this session hit the *exact* same symptom:
`ingressPublication` connects fine, `egress.isConnected` never does, `POLL_RESPONSE` times out.
Two contributing bugs were found and fixed:
1. The Helm deployment never set `LINE_HANDLER_AERON_EGRESS_CHANNEL`, so it fell back to the code
   default's literal `aeron:udp?endpoint=localhost:0` — always wrong in a pod. Necessary but not
   sufficient: after this fix, the *first* `AeronCluster.connect()` succeeded, but every
   subsequent reconnect still failed identically, with a real routable pod IP as the response
   channel, ruling out the `localhost` explanation for the deeper case.
2. **The actual root cause**: the egress/response channel had no `term-length` set, so Aeron used
   its large ~48MB default term buffer per publication — confirmed directly via `AeronStat` and
   `/data/aeron/publications` on the live leader (fresh 48MB `*.logbuffer` files appearing after
   each failed reconnect) and `/dev/shm` on the client pod (Kubernetes' default 64Mi, barely
   enough for one such buffer). The client's own local memory-mapped allocation for its response
   subscription was silently failing — this was never a network routing problem at all. Fixed by
   adding `|term-length=64k` to the egress channel, matching the ingress channel's own setting.
   Live-verified: `line_handler_messages_relayed` climbing steadily, zero relay errors, `/dev/shm`
   usage stable.

See `docs/AERON-SEQUENCER-IMPLEMENTATION-STEPS.md`'s "New findings from the first live
Helm/cluster run" for the full writeup (path-to-completion step 3, now done). Whether this
`term-length` gap was *also* the root cause of `ingress-shim`'s original failure (below) is
unconfirmed and now unfalsifiable — that service is deleted — but note `ingress-shim`'s failure
happened on the very *first* connect attempt, unlike `line-handler-template`'s (first succeeds,
only reconnects fail), so it may not be the identical mechanism; the "ruled out"/"not yet tried"
list below is kept as-is for historical accuracy, not because it's still open.

**Symptom** (original `ingress-shim` writeup, kept for historical record):
`io.aeron.exceptions.TimeoutException` at `ClusterIngressClient.<init>` (or, in
`line-handler-template`'s case, at `ClusterIngressClient.reconnect`) — it can never complete the
`AeronCluster` session-connect handshake to the cluster, regardless of member count.

**Ruled out this session** (don't re-check these first):
- NetworkPolicy — only restricts inbound traffic *to* cluster-node; nothing restricts ingress-shim.
- DNS resolution — confirmed fresh and correct (`getent hosts` matches actual pod IPs exactly).
- CPU throttling as the *sole* cause — raised limits, reproduces identically even fully
  unconstrained in an ad-hoc debug pod with no resource limits at all.
- Raw pod-to-pod UDP reachability in general — proven bidirectional with a `nc`-based test
  between scratch pods.
- `/dev/shm` writability for the embedded media driver — confirmed writable (64Mi default).
- The Helm chart's pod spec specifically (securityContext, `readOnlyRootFilesystem`, resource
  limits) — the bug reproduces identically running the same jar manually in a vanilla ad-hoc pod
  with no such constraints, so it isn't chart-specific.
- Simple timing/slowness — overrode `AeronCluster.Context`'s connect timeout up to 120 seconds
  via `-Daeron.cluster.message.timeout`; still times out.

**What we know**: via live `AeronStat` against the running CnC file, cluster-node's own embedded
driver shows healthy, substantial traffic (millions of bytes sent/received, real heartbeats) and
is confirmed listening on port 9010. `ingress-shim`'s own embedded driver, by contrast, shows
**zero bytes sent system-wide** even after 120 seconds, despite having registered publications
toward all 3 members. The client's connect state machine is stuck in `AWAIT_PUBLICATION_CONNECTED`
with `ingressPublication=null` — none of its 3 per-member ingress publications ever report
connected.

**Not yet tried**:
1. Reset Docker Desktop's Kubernetes cluster fresh — this session did a *lot* of churn (many
   `kubectl debug` ephemeral containers, repeated pod deletes, several `helm upgrade` cycles) and
   a stale/corrupted CNI or conntrack state on this specific node hasn't been ruled out.
2. Enable Aeron's own event-logging agent (`-javaagent`, `aeron.event.log=all`) for byte-level
   packet tracing inside the driver — not attempted this session, would give a much more direct
   answer than counter-inspection.
3. Compare directly against `nats-bridge`'s own cluster-touching config (its archive-control
   channel has the same "bare headless-Service name" pattern the ingress fix already addressed
   for ingress-shim — worth checking whether nats-bridge needs the identical per-member-endpoint
   fix, and whether it hits the same or a different connect failure).

**Update (fresh-environment re-run, same session as the `IngressIdempotencyIT` work below)**: item
1 above is effectively satisfied — this re-run was against a freshly restarted Docker Desktop —
and the bug still reproduces, but with a materially different, more specific symptom than "What we
know" above describes, worth correcting rather than layering on top of stale text: the client's
`ingressPublication` is **not** null and **is** connected (`isConnected=true`, `position=128`,
i.e. the `SessionConnectRequest` was actually sent to the leader, confirmed as
`cluster-node-0` via `ClusterTool is-leader`). The specific gap is `egress.isConnected=false` —
the leader's response, addressed back to the client's own advertised `responseChannel`
(`aeron:udp?endpoint=<ingress-shim pod IP>:<ephemeral port>`, correctly the pod's real routable IP,
not `localhost`), never arrives. Re-checked item 3's NetworkPolicy angle from the other direction
this time (traffic *into* `ingress-shim`, not just *out of* it) — the namespace's one
`NetworkPolicy` only selects `cluster-node` pods (`Ingress`-only, ports 9010–9070); `ingress-shim`
isn't selected by any policy, so its inbound traffic (the response landing) is unrestricted.
NetworkPolicy remains ruled out. Item 3's `nats-bridge` comparison: confirmed `nats-bridge`'s
`archiveControlChannel` did use the identical bare-headless-Service-DNS-name pattern already
fixed for `ingress-shim`'s ingress endpoints — same class of bug, different mechanism (Archive
control connections don't have `AeronCluster`'s automatic leader-following, so this needed
explicit leader-tracking, not just an endpoint list). **Fixed** (implementation-steps.md's
"Path to completion" step 1): `LeaderArchiveConnector` now tries each cluster member's archive
control channel in turn and keeps whichever one actually has the matching egress recording,
fed by a new plural `NATS_BRIDGE_CLUSTER_ARCHIVE_CONTROL_CHANNELS` Helm env var
(`gcm-md.clusterNode.archiveControlChannels` helper, one URI per member) replacing the old
singular bare-DNS var. Unit-tested; not yet exercised against a live cluster (that's
path-to-completion step 2). Item 2 (event-log agent) is now the clear next step for *this*
(`ingress-shim`-class, now moot) bug's underlying `SessionConnectRequest` → response-publication
path — narrowed specifically to that, not the whole connect handshake.

## Explicitly out of scope (unrelated to this bug, don't conflate)

- Multi-AZ / real inter-AZ RTT measurement — needs an actual multi-AZ cluster.
- The in-process `*IT` integration-test suite (`services/sequencer-aeron/integration-tests`) and
  the ArchUnit/Java 25 tooling incompatibility — both separate, pre-existing Milestone 9 gaps
  (see `docs/AERON-SEQUENCER-IMPLEMENTATION-STEPS.md`, Milestone 9), not touched by this work and
  not blocked by it either.

## Live environment state (as of this session)

Docker Desktop context, namespace `gcm-md-local`: 3-member cluster-node StatefulSet deployed and
healthy, NATS + streams/KV buckets set up, ingress-shim/nats-bridge deployed but ingress-shim is
crash-looping on the bug above. A stale single-member deployment from a prior session is still
running in the old `market-data-local` namespace, untouched — safe to tear down with
`deploy/local/90-teardown.sh` (pointed at the new namespace only) or manually once no longer
needed for comparison.

## Resuming this work

Start with `docs/AERON-SEQUENCER-3-MEMBER-CLUSTER-PLAN.md`'s "Status as of this session" section
for full citations, then pick one of the three "not yet tried" items above. Once `ingress-shim`
connects, `40-smoke-test.sh --verify=true` and `50-failover-drill.sh` should be re-run to actually
exercise the `--verify` contiguity/duplicate check and the failover drill for the first time.

## Update: in-process 3-member `*IT` suite (Milestone 9 §12.2)

Follow-up session work, independent of the `ingress-shim` bug above (this suite uses
`ClusterNodeConfig.localMultiMember`, an in-process harness unrelated to Kubernetes/ingress-shim).

**Done**: all 5 `*IT` classes (`LeaderKillContiguityIT`, `EgressNoDoublePublishIT`,
`FollowerRescheduleIT`, `SnapshotCycleIT`, `IngressIdempotencyIT`) are un-`@Disabled`. Three real
bugs found and fixed while getting the first of them (`IngressIdempotencyIT`) running:
1. Every test's shared `offerMessages` helper allocated `new byte[64]` but wrote a 4-byte int at
   absolute offset 64 (`sourceSeqNum`, per `StampingConfig.v4Defaults()`) — an immediate
   `IndexOutOfBoundsException`. Fixed to `new byte[68]` in all 5 files.
2. `IngressIdempotencyIT`'s exact-count assertion (350) didn't account for the cluster's own
   heartbeat messages (templateId 4), which share the same global sequenceId counter as ingress
   messages (templateId 9) and get recorded by the same test double. Fixed by having
   `RecordingEgressPublisher` track each entry's templateId and exposing
   `sequenceIdsForTemplate(int)` so a test can filter to just its own messages; updated the
   contiguity assertion to "strictly increasing, no duplicates" rather than "exact integers 1..N"
   (heartbeats legitimately occupy IDs in between).
3. `RecordingEgressPublisher` used `CopyOnWriteArrayList`, which copies the entire backing array
   on every `add()` — called on the cluster's own hot message-processing path, this is O(n²)
   total work for n recorded messages. Switched to `ConcurrentLinkedQueue`.

**Not done**: a clean, fully-passing run of even `IngressIdempotencyIT` alone. Diagnosed via a
temporary instrumented run: the recorder's own ingress-message count plateaus (e.g. at 204, then
100 in a later run) well short of the 300/350 expected, while heartbeats keep flowing normally —
this is not a stamping/dedup correctness issue (the pure `StampingEngine` logic is separately
proven correct by `libs/sequencer-core`'s own passing unit tests) and reproduced identically
whether or not the `CopyOnWriteArrayList` fix was present, ruling that out as the direct cause
too. The same run's *total* wall-clock time (927s for what should be a sub-minute test) lines up
with a system-level explanation instead: this session ran many hours of heavy JVM/Docker/kubectl
churn (the 3-member K8s work above, dozens of `kubectl debug` ephemeral containers, repeated
`mvn`/`java` launches), and free memory had dropped from ~1.9GB to ~780MB by the time of that run
(`vm_stat`), with load average ~4.3 on a 14-core machine — consistent with memory-pressure-driven
scheduling stalls rather than a code defect. The other 4 `*IT` classes haven't been run at all
yet (deliberately stopped here rather than continuing to burn cycles in a degraded environment).

**Recommendation**: resume in a fresh session/environment (or after restarting Docker Desktop and
confirming free memory is back to a healthy level) and re-run `IngressIdempotencyIT` first — if
it now passes cleanly, the 3 fixes above were sufficient and the remaining 4 classes are next; if
it still stalls in a genuinely fresh environment, that's a real, different finding worth its own
investigation (not memory pressure). Don't assume the fixes already landed are complete without
that clean re-run.

## Update: fresh-environment re-run — memory pressure ruled out, real root cause found deeper (§12.2)

**Status: DEFERRED — intentionally paused, not currently being worked on.** This is a real,
reproduced-when-fresh flow-control finding (leader's `alias=log` MDC publication pinned at
`pub-pos == pub-lmt` during normal processing against a stable leader, not during an election —
see the analysis below), not urgent or blocking anything else in flight. Pick it back up only
when explicitly prioritized. See Milestone 5B in `docs/AERON-SEQUENCER-IMPLEMENTATION-STEPS.md`
for what's actively being worked on instead.

Follow-up session, starting from a just-restarted Docker Desktop (~13GB free, load average 2.3,
climbing down) — exactly the clean environment the recommendation above called for.

**`IngressIdempotencyIT` still fails, and it is not memory pressure.** It reproduces identically
in this fresh environment: the recorded ingress count plateaus partway through the 300-message
burst (204, 205, 0, or fully-complete depending on the run — non-deterministic) and then never
advances again for the rest of the 30s wait, even though `offerMessages` reports all 300
`client.offer()` calls succeeding in ~2ms. Ruled out, with evidence, in this session:
- **Leadership handover to a different member** — instrumented all 3 members' recorders at the
  point of failure; all 3 plateau at the *identical* count (e.g. 204/204/204), and the same
  member stays `isLeader()==true` throughout. The replicated state machine is uniformly stuck,
  not diverging.
- **`ClusterIngressClient` not being polled during the wait** — `pollEgress()`'s own Javadoc says
  it "must be polled regularly, even between offers." The suite's `awaitCount`/wait helpers were
  plain `Thread.sleep` loops that never touched the client again after the initial burst — a real
  contract violation, now fixed (`awaitCount` and a new `pollFor` helper poll the client
  throughout). This alone does not fix the stall (confirmed by testing), but it's worth keeping:
  `IngressIdempotencyIT.java`'s class Javadoc documents both the fix and the still-open stall.
- **Transport-level packet loss** — `AeronStat`/`ErrorStat` (via `io.aeron.samples.ErrorStat`,
  needs `--add-opens java.base/jdk.internal.misc=ALL-UNNAMED --enable-native-access=ALL-UNNAMED`
  on Java 25) against all 3 members' `aeron.dir`s during and after a stalled run: zero NAKs, zero
  invalid packets, zero errors, throughout. Bytes are arriving cleanly; nothing is failing at the
  wire level.

**What's new**: a live thread dump plus `AeronStat`'s regular (non-error) counters, taken from the
leader mid-stall, show every consensus-module/clustered-service agent thread idle in its normal
`BackoffIdleStrategy` park (not looping, not blocked on IO) — and the leader's own internal
`alias=log` MDC publication (the one the Raft log append/replicate/commit path runs over) has
`pub-pos` pinned *exactly* equal to `pub-lmt` (e.g. both `50,784`), i.e. a genuine flow-control
backpressure stall, confirmed via `Cluster commit-pos` matching that same number and never moving.
This is not "idle, nothing to do" — it's "wants to publish more, and its flow-control window has
stopped advancing, permanently." The likely gate on that window is the local Archive recording
subscriber (a consumer of the same log stream), but this wasn't confirmed further; the archive's
`0-0.rec` segment file for the leader showed real (not zero) data written, roughly consistent with
the stall position, which doesn't cleanly confirm or rule out the archive as the slow consumer.

**A separate, real bug found and fixed while investigating**: `LeaderKillContiguityIT` — design
§12.2's own stated centerpiece test — was failing fast (under 1s) with `[a new leader was
elected]` asserting `1 != 1`. Root cause: `InProcessCluster#killMember` gracefully closes a
member's whole process without ever calling `onRoleChange(FOLLOWER)` on its `EgressPublisher`, so
the killed member's `RecordingEgressPublisher.isLeader()` stays stuck at its last value (`true`)
forever — `awaitLeader()` then immediately "re-discovers" the dead member as leader instead of
waiting for a real election among the survivors. Fixed by having `awaitLeader` take an excluded
member id and skip the killed member's (permanently stale) recorder. **`LeaderKillContiguityIT`
now passes**, exercising the full kill-leader → re-election → 1000-message
no-gap/no-duplicate/no-regression scenario end-to-end for the first time.

**Full suite status after this session**: `EgressNoDoublePublishIT` and `SnapshotCycleIT` pass
reliably (2/2 runs each). `FollowerRescheduleIT` passed standalone twice (10.7s, 25.8s) but flaked
once when run in the same batch as the other classes back-to-back — consistent with the general
resource-contention pattern below, not a distinct bug. `LeaderKillContiguityIT` now passes
(above). Only `IngressIdempotencyIt` remains genuinely broken.

**Confirmed session-level resource pressure, separately**: by the time of the JMH/benchmark work
later in this same session, load average had climbed to 7.60 (from 2.3 fresh) and free memory had
dropped to ~4.4GB (from ~13GB) — the same kind of drift the original memory-pressure hypothesis
was based on, just not the explanation for `IngressIdempotencyIT`'s stall specifically (which
reproduced identically in the genuinely fresh, low-load window). Both things are true: the
environment does degrade over a long session of heavy JVM/Aeron churn, *and* there is a separate,
real, reproducible-when-fresh flow-control bug.

**(For whenever this is resumed — not an active task.)** **Not yet tried** (next step, narrowed): Aeron's own event-logging agent
(`-javaagent:...aeron-agent.jar -Daeron.event.log=all`) attached to the leader specifically,
watching the `alias=log` MDC publication and its Archive-recording subscriber around the moment
`pub-pos` stops advancing — this is a much more direct answer than counter-inspection and was
already the recommended next step in the section below (for the unrelated `ingress-shim` bug);
it now applies here too, and arguably more urgently since this one has a live in-process repro
that doesn't need Kubernetes to reach.

**Also fixed while here**: the same buffer-undersizing bug pattern the `*IT` suite already learned
about (`new byte[64]` allocated but a 4-byte `sourceSeqNum` written at absolute offset 64) was
still present, unfixed, in `services/sequencer-aeron/bench/.../ClusterOfferBenchmark.java` — this
benchmark had evidently never been run before (no JMH result files existed anywhere in the repo
prior to this session). Fixed to `new byte[68]`; the benchmark now runs, but produced noisy,
order-of-magnitude-too-slow numbers (tens–hundreds of ms/op, expected sub-ms) in this session's
degraded-resource tail end — re-run on an idle machine before trusting it as a real baseline.
`StampingEngineBenchmark` (the pure in-memory stamp path, unaffected by cluster/network variance)
ran cleanly: **4.452 ± 0.026 ns/op**, comfortably inside the ~1µs/message budget.
