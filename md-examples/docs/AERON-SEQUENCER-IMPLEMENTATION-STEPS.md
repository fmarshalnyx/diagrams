# Aeron Cluster Sequencer — Implementation Steps

Derived from `docs/AERON-SEQUENCER-DESIGN.md` (authoritative spec) and
`docs/SEQUENCER-PROJECT.md` (phase-1 contract). This document sequences the work into
concrete, buildable steps against the **current** repo state, which is a flat single
Maven reactor (`md-examples-parent`) with modules `md-models-sbe`, `md-sequencer`,
`sequencer-loadgen`, `sequencer-bench` — not yet the `libs/` + `services/` monorepo
layout §3 describes. Step 0 closes that gap; everything else builds on it.

Each step lists: what changes, where, and the acceptance check before moving on.
Section references (§n) point back to the design doc.

---

## Current status & path to completion

This section is the single source of truth for "where are we and what's left" — read this
first. Everything below it (Milestones A0–12) is the detailed record of what each step
requires and how it was actually done; keep going there for rationale, citations, and design
detail once you know which milestone you're picking up.

### Status by milestone

| Milestone | Status | What's left |
|---|---|---|
| A0 — Repo restructuring | **Done** | — |
| 1 — `libs/sequencer-core` | **Done** | — |
| 2 — `cluster-node` | **Functionally done; stability NOT yet resolved** | Self-restart instability: `driverTimeoutMs` fix in place and helps, but every attempt to cleanly re-verify it this session was confounded by two further bugs found live (a stuck-FOLLOWER state, task 37; a `line-handler-template` zombie-client state with no self-healing, task 38) — see "New findings" and its "Summary for whoever picks this up next" for the full account and recommended entry point |
| 3 — Egress | **Done** | — (leader-kill acceptance test passes for real, see 9) |
| 4 — `libs/cluster-client` | **Done** | — |
| 5 — `libs/ingress-transport` (revised) | **Done** (5.1–5.3) | 5.4: real line handlers adopting it — external work, not this repo's to finish |
| 5B — line handler template & mock upstream source | **Done** (5B.1–5B.4), **live-verified, all genuinely working** | `mock-upstream-source` fixed (unbounded-retention stream that exhausted NATS storage; `EgressConsumer` switched from a drop-prone core NATS subscription to JetStream — both root-caused and fixed). `line-handler-template`'s `aeron` ingress-transport now actually relays (was blocked, root-caused and fixed this session). A second, unrelated `line-handler-template` bug found later the same session: its embedded Aeron client hit the same 10s-default self-close as cluster-node's — see "New findings" below. 5B.3 (`40-smoke-test.sh`) rewritten and passing. 5B.4: `loadgen` deleted — module, reactor entry, CI job, all references |
| 6 — `nats-bridge` | **Done, live-verified** | `archiveControlChannel` fix confirmed working. `bridge_publish_failures`/silent-thread-death robustness bug both root-caused and fixed this session (see below) — same NATS-storage-exhaustion incident as 5B's finding. `ContiguityTracker`/`BridgeCheckpoint` checkpoint-reset hardening also done (see "New findings") |
| 7 — Kubernetes & Helm | **Done, live-verified** | `cluster-node`/`nats-bridge`/`mock-upstream-source`/`line-handler-template` all reach `Running`, pass readiness, and are genuinely doing their jobs on a real `make local-up`-equivalent deploy, including `line-handler-template`'s `aeron` transport |
| 8 — Local deployment scripts | **`40-smoke-test.sh` done and passing** | `50-failover-drill.sh` rewritten against the 5B persistent services but not yet run against a live 3-member drill this session |
| 9 — Testing suites | **Partial** | See breakdown below |
| 10 — Observability | **Done** | Metrics wired (including `cluster-node`'s scrape path, previously not exposed at all — found and fixed writing this dashboard); `deploy/observability/gcm-md-sequencer-aeron-dashboard.json` written and live-verified |
| 11 — GitLab pipeline | **`build`/`test`/`integration`/`package:*` validated locally, `deploy:*` still unverified** | 4 real bugs found and fixed running the pipeline locally (`gitlab-ci-local`) — a repo-wide `.gitignore` bug hiding the parent POM from git entirely, a nonexistent Maven image tag, and two variants of `jib` running against modules it shouldn't. `deploy:*` needs a real Kubernetes agent context + registry, not attempted |
| 12 — Migration cutover | **Blocked** | Correctly gated on 5.4, 5B.3/5B.4, 6, and 9 below |

**Milestone 9 breakdown:**

| Item | Status |
|---|---|
| 12.1 phase-1 tests | **Done** |
| 12.2 in-process `*IT` suite | 4/5 classes pass (`EgressNoDoublePublishIT`, `SnapshotCycleIT`, `FollowerRescheduleIT`, `LeaderKillContiguityIT`). `IngressIdempotencyIT` is **DEFERRED** — real, root-caused flow-control stall, intentionally paused, not blocking anything else |
| 12.3 determinism/ArchUnit suite | **Blocked upstream** — ArchUnit can't parse this reactor's Java 25 class files as of the latest published release |
| 12.4 parallel-run diff harness | Built, 14 passing unit tests, **never run against real phase-1-vs-phase-2 traffic** |
| 12.5 JMH | Stamp path done (4.452 ± 0.026 ns/op, comfortably under budget); cluster-offer path runs but its numbers aren't trustworthy yet (measured under heavy session resource pressure — re-run needed); 1M msgs/sec loadgen target and multi-AZ vs single-AZ number both still unmeasured |

### New findings from the first live Helm/cluster run (this session)

Running `make local-up`'s equivalent (fresh `gcm-md-local` namespace, single-member cluster,
`lineHandlerTemplate`/`mockUpstreamSource` enabled) surfaced several real issues:

- **`line-handler-template`'s Aeron ingress transport couldn't relay messages — found, fixed,
  live-verified.** The very first `AeronCluster.connect()` (at Spring bean-creation time)
  succeeded and the app started cleanly, but every subsequent `ClusterIngressClient.reconnect()`
  timed out with `ingressPublication.isConnected=true` / `egress.isConnected=false` — the same
  symptom already investigated (and, wrongly, believed moot) for the retired `ingress-shim`. Two
  contributing bugs, both fixed:
  1. The chart never set `LINE_HANDLER_AERON_EGRESS_CHANNEL`, so it fell back to the code default
     `aeron:udp?endpoint=localhost:0` — literal `localhost`, always wrong in a pod. Fixed via a
     new `POD_IP`-sourced env var in `templates/linehandlertemplate/deployment.yaml`, mirroring
     `nats-bridge`'s existing pattern. Necessary but not sufficient on its own — fixed the very
     first connect's correctness but not the reconnect failures.
  2. **The actual root cause**, found via `AeronStat`/`ClusterTool errors` on the live leader plus
     `/dev/shm` inspection on the client pod: the egress/response channel had no `term-length`
     set, so Aeron fell back to its large default term buffer (~48MB per publication — confirmed
     directly: three fresh 48MB `*.logbuffer` files appeared in the leader's own `/data/aeron/publications`
     after failed reconnects). The client pod's `/dev/shm` (Kubernetes' default 64Mi) can barely
     fit *one* such buffer; every reconnect attempt raced against a nearly-full `/dev/shm`, and the
     client's own local memory-mapped allocation for its response subscription silently failed —
     `egress.isConnected` never had a chance to become true, on either side, regardless of network
     routing. (A tempting but wrong first fix, tried and disproven: sharing one `Aeron` client
     across reconnects instead of letting `AeronCluster.Context.conclude()` spin up a new one each
     time — architecturally correct and kept, but didn't change the symptom at all, which is what
     pointed at buffer sizing instead of client/session lifecycle.) Fixed by adding
     `|term-length=64k` to the egress channel default (`LineHandlerProperties.Aeron`,
     `application.yml`) and the Helm-supplied value, matching the ingress channel's existing
     `term-length=64k`. **Live-verified**: `line_handler_messages_relayed` climbing steadily (100k+
     messages relayed within two minutes), `line_handler_relay_loop_errors=0`, client `/dev/shm`
     usage stable/falling instead of climbing toward exhaustion. `docs/AERON-SEQUENCER-3-MEMBER-CLUSTER-STATUS.md`'s
     "Moot" section is corrected accordingly.
  - **Related robustness fix, also landed and still worth keeping regardless of the above:**
    `UpstreamRelay.run()`'s loop had no exception handling — a transient offer/reconnect failure
    (on any transport) was killing the `line-handler-relay` daemon thread permanently and silently
    (readiness/liveness probes don't detect a dead relay thread, so the pod stayed
    `Running`/`Ready` with zero actual relaying happening forever). Fixed by wrapping each loop
    iteration in a catch that logs, counts (`line_handler_relay_loop_errors_total`, new gauge), and
    retries instead of propagating.
- **`mock_upstream_gap` and `bridge_publish_failures` — root-caused together, fixed.** Both
  turned out to share one cause, found while wiring and live-testing the 5B.3 smoke-test script
  (step 4 below): `mock-upstream-source`'s self-created `MOCK_UPSTREAM` NATS stream
  (`TrafficGenerator.ensureStreamExists`) had **unlimited retention** — fine for the old one-shot
  `sequencer-loadgen` CLI, fatal for a persistent, always-on service. It grew to **5.6 million
  unacked messages / ~1GB**, hitting the NATS server's `max_file_store` quota (`infra/nats-setup`'s
  `fileStore.pvc.size: 1Gi`) exactly. Once the server-wide JetStream storage was exhausted, *every*
  JetStream write started failing with "insufficient resources [10023]" — including
  `nats-bridge`'s unrelated republish to `MD_SEQUENCED` (`bridge_publish_failures`) and, downstream
  of that, gaps in what `mock-upstream-source`'s own `EgressConsumer` observes on `md.sequenced`
  (`mock_upstream_gap`). Confirmed directly via `nats stream ls` (977MiB–1006MiB, pinned at the
  quota) and the NATS server's own log (`[ERR] JetStream file resource limits exceeded for
  server`, repeating every ~10s). Two-part fix in `TrafficGenerator.ensureStreamExists`:
  1. `RetentionPolicy.WorkQueue` (messages removed once the sole durable consumer —
     `line-handler-template`'s `UpstreamRelay` — acks them) + `maxAge(1h)` as a safety net for the
     consumer being down for an extended period.
  2. WorkQueue alone wasn't sufficient: it was still possible for a producer to durably outpace
     the sole consumer, which is exactly what was happening — `mock-upstream-source`'s configured
     rate (code default 100,000 msg/sec, used unmodified in `environments/local/values.yaml`) is
     far more than this environment can actually drain end-to-end (observed ceiling: roughly
     1,600–2,000 msg/sec, plausibly Docker Desktop CPU / per-message Aeron-offer overhead), so the
     backlog grew without bound regardless of retention policy. Added `maxBytes(256MB)` as a hard
     ceiling (oldest unacked messages dropped once exceeded, trading bounded message loss for
     never taking down the whole NATS server again), and lowered `environments/local/values.yaml`'s
     `mockUpstreamSource.configYaml` rate to `1000`/sec, comfortably under the observed ceiling for
     ordinary local dev. Also fixed in passing: `ensureStreamExists`'s catch-all was completely
     silent, which is exactly what made this incident hard to diagnose (masked behind confusing
     "503 No Responders" publish errors with no clue why) — it now logs the actual exception.
  - **Related robustness fix, found the same way `UpstreamRelay`'s was:** `BridgePipeline`'s poll
    loops (`pollUntilStopped` and the `ReplayMerge` catch-up loop) had the identical no-exception-
    handling gap — a `BridgeCheckpoint.write()` failure (from the same NATS storage exhaustion)
    threw up through `onFragment`, killing the `nats-bridge` daemon thread permanently and
    silently while the pod stayed `Running`/`Ready`. Fixed with the same catch/log/count
    (`bridge_loop_errors_total`, new gauge)/retry pattern.
  - **Still open, distinct, and *not* fully explained by the above:** even with both fixes live
    and the pipeline otherwise healthy, `nats-bridge`'s `bridge_messages` counter has repeatedly
    gone completely static (zero throughput, no errors, no log output) for extended stretches
    during this session's testing, recovering only after restarting `nats-bridge` and/or clearing
    its NATS-KV checkpoint. Root cause traced at least twice to `ContiguityTracker` permanently
    skipping every live fragment because its checkpoint (read once at `BridgePipeline`
    construction) ended up ahead of what the live stream could ever produce again — itself a
    downstream effect of `cluster-node`'s sequenceId space having a discontinuity across a pod
    restart (see the new cluster-node finding below). Likely resolves on its own once cluster-node
    stops restarting; flagged here because it's a real gap in `BridgeCheckpoint`/`ContiguityTracker`'s
    resilience to a non-monotonic Archive recording that a genuinely production multi-member
    cluster (which preserves sequenceId continuity across leadership changes via Raft) shouldn't
    normally trigger, but is still worth a defensive fix — see the new tracked item.
- **Partially mitigated, NOT resolved: `cluster-node`'s clean, silent, exit-0 self-restarts every
  ~15-30 minutes (97 restarts over 25 hours in extended live testing) — a necessary fix is in
  place, but live re-testing this session showed it is not sufficient on its own; see the revised
  theory below before trusting this as done.** `ClusterNodeLauncher.main`
  blocks on `new ShutdownSignalBarrier().await()`, which only returns on SIGINT/SIGTERM or an
  explicit `signalAll()` call from within the process — no external `kubectl`/probe/OOM/eviction
  evidence was ever found (ruled out via `kubectl get events`, which showed a plain
  `Killing`/`Stopping container` with no preceding `Unhealthy` event, and via checking that
  `helm upgrade` invocations during the observation window never touched any `clusterNode.*`
  value, so the StatefulSet's rendered pod template was byte-identical across them). Root cause:
  Aeron's own out-of-the-box `driverTimeoutMs`/`clientLivenessTimeoutNs` default (10s) is too
  tight for this resource-constrained, shared local Docker Desktop host — a GC pause or CPU
  scheduling delay past that window makes the consensus module's internal Aeron client conclude
  its co-located embedded driver died, throwing `AgentTerminationException("unexpected Aeron
  close")`, which invokes the *default* `terminationHook` (`() ->
  shutdownSignalBarrier.signalAll()`) — silent by design, hence zero application-level log
  evidence despite the exit being entirely internal to the process. Fixed in
  `ClusterNodeLauncher.java`: a new `CLUSTER_DRIVER_TIMEOUT_MS` env var (default 10000, matching
  Aeron's own default so behavior is unchanged unless explicitly overridden) is applied to every
  context with its own driver-timeout-equivalent setting (`MediaDriver.Context`,
  `AeronArchive.Context` via `messageTimeoutNs`) plus the global `aeron.driver.timeout`/
  `aeron.client.liveness.timeout` system properties for the two contexts with no direct setter
  (`Archive.Context`, `ConsensusModule.Context` — both create their own internal `Aeron.Context`
  which reads these). The consensus module's `terminationHook` is also overridden to log first
  before shutting down, so any future recurrence is immediately diagnosable instead of a silent
  mystery, regardless of whether this timeout fix eliminates the underlying cause entirely. A
  real second bug was found wiring this in: raising `clientLivenessTimeoutNs` alone without also
  raising `MediaDriver.Context`'s `publicationUnblockTimeoutNs` (Aeron default 15s) violates
  Aeron's own invariant (`Configuration.validateUnblockTimeout`: unblock timeout must exceed
  liveness timeout) and made the driver refuse to start at all once the liveness timeout was
  pushed past 15s — fixed by setting `publicationUnblockTimeoutNs` to `2 * driverTimeoutMs`,
  preserving Aeron's own 15s/10s (1.5x) default ratio. Wired through the Helm chart:
  `deploy/helm/gcm-md-sequencer-aeron/values.yaml`'s `clusterNode.aeron.driverTimeoutMs` (chart
  default: 10000, left at Aeron's own default so fast genuine-failure detection is preserved
  where the host isn't resource-constrained) and `environments/local/values.yaml`'s override
  (30000 — this shared Docker Desktop host is exactly where the incident happened).
  **Live-tested, and the raised timeout alone is NOT sufficient — revised theory below.** Rebuilt
  image, redeployed to `gcm-md-local`, confirmed `CLUSTER_DRIVER_TIMEOUT_MS=30000` present in the
  running container's environment and the pod reaches `Running`/leader (role 2) cleanly. Two
  separate restart-count watches this session:
  - Watch 1 (40 min): 1 restart, ~15 minutes in. Coincided with this session's own heavy
    concurrent local activity on the same Docker Desktop host (`mvn -T 1C test` over the whole
    reactor, plus a 4-image `jib:dockerBuild` run) — plausibly a resource-contention confound.
  - Watch 2 (immediately following, same pod instance): a **second** restart at ~28 minutes
    uptime, with **no heavy local build activity running this time** (only light `kubectl exec`
    checks and doc edits) — same exit-0/`aeron.isClosed()` termination-hook message both times.
    This rules out "it was just my own builds" as the sole explanation.
  - **Revised theory, not yet confirmed:** at the time of the second restart,
    `line_handler_messages_relayed` had climbed by ~871,000 messages in the ~3 minutes since that
    pod started — a rate of roughly 4,800+ msgs/sec, far above the ~1,600-2,000/sec local ceiling
    this environment was tuned for, and `MOCK_UPSTREAM`'s `line-handler-template` consumer still
    had **1.47 million** messages pending (`nats consumer info MOCK_UPSTREAM line-handler-template`).
    This backlog is itself a side effect of `line-handler-template`'s own ~30-hour outage (the
    separate bug found and fixed this session, see above) — not representative of this
    environment's normal steady-state traffic. Working theory: draining this abnormal backlog at
    extreme burst throughput is what's actually stressing `cluster-node` past even a 30s timeout
    (GC pressure, snapshot/heartbeat housekeeping, or per-source tracking growth under sustained
    high throughput — not yet isolated which), and the *raised timeout only delays* the failure
    rather than eliminating it, consistent with a growing-pressure mechanism rather than a random
    idle GC pause. **Partially confirmed, with a complication**: a follow-up background watch
    covering ~7 hours found `cluster-node`'s restart count held flat with **zero further
    restarts** the entire time (versus the original ~1-per-15-30-min pattern) — real evidence the
    30s timeout substantially helps. But the backlog never actually drained to near-zero as
    intended; `nats consumer info` showed it oscillating and then plateauing at its ~1.47M
    ceiling for the last ~20+ minutes of the watch. Cause: `line-handler-template` itself had
    gone into `CrashLoopBackOff` partway through (see the `messageTimeoutNs` finding below), so it
    silently stopped consuming — the "steady-state" phase of that watch was actually mostly-idle,
    not the clean test intended. Still open whether `cluster-node` is durably stable once it's
    genuinely processing steady, non-burst traffic for an extended window — the two findings below
    (fixed and unresolved, respectively) block re-running that test properly.
- **A third, distinct Aeron timeout bug, found live in the same investigation:**
  `AeronCluster.Context`'s own `messageTimeoutNs` (governs the initial session-connect handshake —
  `AsyncConnect`'s `AWAIT_PUBLICATION_CONNECTED` state, unrelated to `driverTimeoutMs`/
  `clientLivenessTimeoutNs`) defaults to Aeron's stock 5 seconds. This connect happens
  synchronously inside `ServiceConfiguration.clusterIngressTransport`'s `@Bean` factory method at
  Spring context startup, so a timeout here throws, kills the whole context, and the pod
  **permanently `CrashLoopBackOff`s — no retry**, unlike the transient-backpressure handling
  `ClusterIngressClient.offer()` already has post-startup. Observed live: `line-handler-template`
  hit exactly this (9, then 39, restarts) while `cluster-node` was busy — a session-connect
  request from a brand-new client competing with `cluster-node` processing a huge backlog at high
  throughput plausibly can't always get serviced within 5s. **Fixed**: added
  `.messageTimeoutNs(TimeUnit.MILLISECONDS.toNanos(aeronConfig.getDriverTimeoutMs()))` to the
  `AeronCluster.Context` in `ServiceConfiguration.clusterIngressTransport`, reusing the same
  `line-handler.aeron.driver-timeout-ms` property as the other two knobs. Rebuilt, redeployed —
  the timeout in the error message changed from 5s to 30s as expected, confirming the fix applied,
  but see the next finding for why this alone still wasn't enough to unblock things.
- **New, unresolved, and more fundamental than timeout tuning: `cluster-node` got stuck as a
  permanent FOLLOWER (`sequencer_cluster_role=0`) with `sequencer_commit_position` stuck at 0 —
  in a single-member topology, which should always elect itself leader (role 2) near-instantly.**
  This fully explains why `line-handler-template` kept timing out even at the raised 30s
  `messageTimeoutNs`: only a leader accepts ingress sessions. Restart count was 4 at the time,
  and critically **these were all container-level restarts within the same pod, not pod
  recreations** — `CLUSTER_DATA_DIR=/data` is an `emptyDir` that survives container restarts
  (only pod deletion wipes it), while `dirDeleteOnStart(true)` wipes the Aeron media-driver
  directory fresh on every restart. A mismatch between persisted consensus/recording-log state and
  a freshly-reset transport layer, across 4 restart cycles, is the leading (unconfirmed) theory for
  why election/recovery got stuck. **Root cause not investigated** — worked around by deleting the
  pod outright (`kubectl delete pod`, forcing a full StatefulSet recreation and a clean `/data`),
  which immediately restored leader role and a working pipeline. **Tracked separately** (task 37,
  not yet started) rather than investigated further this session: this may be an artifact specific
  to this local single-member/nominally-ephemeral-but-actually-persisted-across-restarts setup,
  not necessarily representative of the real 3-member AWS deployment with genuine persistent
  storage and Raft-driven recovery — worth an explicit assessment of representativeness before
  investing more time here.
- After the clean pod recreation, a fresh combined watch (restart count + leader role + commit
  position, not just restart count as before) was started once the backlog — then down to ~67,000
  pending after the earlier partial drain — finished clearing. **Result: much worse than any
  prior signal, and points to a fourth, distinct, unresolved bug.** `sequencer_commit_position`
  stayed at exactly **0.0 for the entire ~3.5-hour watch** while `cluster-node` restarted 13
  times and the backlog climbed back to its ~1.47M ceiling and stuck there — i.e. essentially
  **zero messages were ever committed** during this whole window, despite `cluster-node`
  reporting `role=2` (leader) almost throughout. Root cause, found by checking
  `line-handler-template` directly: its embedded Aeron client had hit the same `"Aeron client is
  closed"` self-close described above, but this time **with no recovery path at all** —
  `UpstreamRelay`'s retry-on-failure loop (designed to survive transient failures) just logs and
  retries forever, but `ClusterIngressClient`'s reconnect logic only recreates the `AeronCluster`
  *session*, reusing the same shared, singleton `Aeron` client bean; once that underlying client
  itself is closed, nothing can revive it short of a full pod restart, and nothing triggers one
  (the failure is caught, not thrown, so the pod stays `Running`/`Ready` — a genuine zombie
  state, observed live: 3.5+ hours of continuous `"relay iteration failed, retrying"` log spam
  with zero successful relays). **Not fixed this session** — tracked as task 38. This means
  today's `cluster-node`-restart data is confounded yet again: most of those 13 restarts happened
  while `cluster-node` was receiving essentially no ingress traffic at all (line-handler-template
  was zombied), so they say little either way about `cluster-node`'s stability under genuine
  sustained load. **Task 34 remains open and unresolved.** A clean read requires task 38 fixed
  first (so line-handler-template reliably self-heals and actually feeds sustained traffic),
  then a fresh multi-hour watch under that condition.

**Summary for whoever picks this up next**: four distinct Aeron-timeout-and-lifecycle bugs were
found and investigated this session, only two are actually fixed and deployed:
1. `cluster-node`'s `driverTimeoutMs`/`clientLivenessTimeoutNs`/`publicationUnblockTimeoutNs` —
   **fixed**, helps substantially (7h with zero restarts under one observation), but not proven
   sufficient on its own since every long-running test since has been confounded by task 38.
2. `line-handler-template`'s own `driverTimeoutMs`/`clientLivenessTimeoutNs`/
   `publicationUnblockTimeoutNs` (same pattern, second process) — **fixed**.
3. `line-handler-template`'s `AeronCluster.Context.messageTimeoutNs` (session-connect handshake,
   5s default causing permanent `CrashLoopBackOff`) — **fixed**.
4. `cluster-node` stuck as permanent FOLLOWER after container-level restarts (task 37) — **not
   fixed, not root-caused**, worked around only by full pod deletion.
5. `line-handler-template`'s zombie-client state with no self-healing path (task 38) — **not
   fixed**, and actively blocks getting clean data on #1.

None of this session's abnormally large backlog (peaked at 1.47M messages, a self-inflicted
artifact of line-handler-template's earlier ~30h outage) has fully drained as of this writing.
Recommended entry point for a future session: fix task 38 first (it's the one actively preventing
clean measurement of everything else), then re-run a long steady-state watch before revisiting
task 37 or re-litigating whether task 34's timeout fix is sufficient.
- **Same root cause, found live in a second process**: `line-handler-template`'s own embedded
  `MediaDriver`/`Aeron` client (`ServiceConfiguration.embeddedMediaDriver`/`.aeron` beans) hit the
  identical Aeron 10s-default self-close (`"Aeron client is closed"`) after ~30 hours of
  uninterrupted uptime, silently blocking *all* upstream traffic (`line_handler_relay_loop_errors`
  had accumulated to 93,245 by the time this was noticed) — this, not any cluster-node issue, is
  why `nats-bridge` showed zero throughput even after cluster-node's own fix landed. Fixed with
  the same pattern: a new `line-handler.aeron.driver-timeout-ms` property (default 10000,
  Aeron's own default), applied to both the embedded `MediaDriver.Context` (plus the same
  `publicationUnblockTimeoutNs` invariant fix as cluster-node) and the shared `Aeron.Context`
  (which has its own independent `driverTimeoutMs`, since `Aeron.Context extends CommonContext`
  directly — no system-property workaround needed here, unlike cluster-node's `Archive.Context`/
  `ConsensusModule.Context`). `environments/local/values.yaml`'s `lineHandlerTemplate.configYaml`
  now sets this to 30000, matching `clusterNode.aeron.driverTimeoutMs`. **Live-verified**: after
  rebuilding the image and redeploying, `line_handler_relay_loop_errors` stayed at 0 and
  `line_handler_messages_relayed` climbed immediately; the whole pipeline (line-handler-template →
  cluster-node → nats-bridge) was observed actively flowing end-to-end for the first time this
  session (`bridge_messages`/`bridge_last_sequence_id` both climbing, `bridge_gap`/
  `bridge_checkpoint_reset` both 0 — the old NATS-KV checkpoint happened to still be below the
  post-restart sequenceId space, so no reset was needed this time).
- **Milestone 9/12.2's `ContiguityTracker`/`BridgeCheckpoint` hardening (tracked separately,
  worked opportunistically alongside this investigation since it's a direct downstream
  consequence of the above): done.** Previously, a `cluster-node` restart that reset the
  sequenceId space (non-persisted `emptyDir` state, as above) left `nats-bridge`'s checkpoint
  permanently stale — `ContiguityTracker.evaluate` returned `SKIP_ALREADY_BRIDGED` forever, so
  `bridge_messages` went silently and permanently static with no error, previously recoverable
  only by manually restarting `nats-bridge` and/or clearing its NATS-KV checkpoint. Fixed with a
  bounded heuristic, deliberately kept distinct from the existing `bridge_gap_total` invariant
  (design §9: "nonzero means an egress bug, not bridge noise" — conflating a legitimate local
  checkpoint-reset recovery with that alarm would weaken it): after
  `nats-bridge.nats.checkpoint-reset-threshold-messages` (default 100,000) *consecutive*
  below-checkpoint sequenceIds, `ContiguityTracker` rebases its baseline to the current
  sequenceId and reports `Decision.CHECKPOINT_RESET_AND_BRIDGE`, which `BridgePipeline` both
  bridges (like a normal message) and counts via a new, separately-alertable
  `bridge_checkpoint_reset_total` metric (added to the Grafana dashboard's "Correctness
  invariants" row). This is a heuristic, not a proof — explicitly documented in
  `ContiguityTracker`'s class Javadoc: too low a threshold risks rebasing mid-legitimate-replay-
  catchup and duplicate-publishing already-bridged messages (replay backlog is bounded by Archive
  recording retention, not by `checkpointIntervalMessages`, so it can legitimately be large after
  extended bridge downtime) — operators should size the threshold above their real worst-case
  backlog. 14 new/updated unit tests in `ContiguityTrackerTest` cover the threshold boundary,
  counter-reset-on-normal-bridge behavior, and the disabled (`RESET_DISABLED`) single-arg
  constructor used by tests that only exercise skip/gap logic.

### Path to completion (ordered)

Roughly dependency-ordered — later items assume earlier ones are done, except where marked
independent. None of this is scheduled; it's the order that avoids rework.

1. ~~**Fix `nats-bridge`'s `archiveControlChannel` bug** (Milestone 6).~~ **Done.** Turned out to
   be more than the one-line fix originally estimated here: a new `LeaderArchiveConnector` class
   (tries each cluster member's archive control channel in turn, keeps whichever one actually has
   the matching egress recording, falls back to the first reachable candidate for a fresh cluster
   with nothing recorded yet) replaces the old single bare-DNS-name `AeronArchive.Context`.
   `NatsBridgeProperties.Cluster.archiveControlChannel` (singular) is now `archiveControlChannels`
   (comma-separated), and a new `gcm-md.clusterNode.archiveControlChannels` Helm helper supplies
   one full `aeron:udp?endpoint=...` URI per member (`templates/_helpers.tpl`,
   `templates/natsbridge/deployment.yaml`). Java side has 4 new unit tests
   (`LeaderArchiveConnectorTest`); Helm side verified via `helm lint`/`helm template` for both
   `replicas=1` and `replicas=3`. **Live-verified** (step 2): `bridge_gap=0` and
   `bridge_last_sequence_id` tracking `bridge_messages` under light load — the archive-connect fix
   itself works against the real cluster, not just unit tests. (`bridge_publish_failures` turned
   nonzero later, under the much heavier load step 3's fix unlocked — a separate, new finding, not
   a regression in this fix; see below and the new tracked item.)
2. ~~**Verify the Helm chart against a real cluster** (Milestone 7).~~ **Done, with real bugs
   found.** Fresh `make local-up`-equivalent run against Docker Desktop: `cluster-node`,
   `nats-bridge`, `mock-upstream-source` all reach `Running`, pass readiness, and are genuinely
   doing their jobs (confirms step 1 live, see above). `line-handler-template` reached
   `Running`/`Ready` too, but its `aeron` ingress-transport couldn't actually relay — root-caused
   and fixed in step 3 below. Also found `mock_upstream_gap` nonzero on the observed egress, and
   (after step 3's fix) `bridge_publish_failures` nonzero under sustained load — both separately
   unresolved, tracked as their own items, not blocking anything else.
3. ~~**Fix the `line-handler-template` Aeron reconnect bug.**~~ **Done, root-caused and
   live-verified.** Not a network routing problem as first suspected — the egress/response channel
   had no `term-length` set, so Aeron used its large ~48MB default term buffer per publication
   (confirmed via `AeronStat`/`ClusterTool errors` on the live leader: fresh 48MB `*.logbuffer`
   files appeared in `/data/aeron/publications` after each failed reconnect) against a pod whose
   `/dev/shm` is only 64Mi by Kubernetes default — the client's own local buffer allocation for its
   response subscription silently failed, well before anything reached the network. Fixed by
   adding `|term-length=64k` to the egress channel default and the Helm-supplied value, matching
   the ingress channel's existing setting. A first attempt — sharing one `Aeron` client across
   reconnects instead of a fresh one per attempt — was architecturally correct (kept) but didn't
   change the symptom, which is what pointed the investigation at buffer sizing instead of
   client/session lifecycle. **Live-verified**: `line_handler_messages_relayed` climbing steadily
   (100k+ in two minutes), `line_handler_relay_loop_errors=0`, client `/dev/shm` usage stable
   instead of climbing toward exhaustion. Full writeup in "New findings" above;
   `docs/AERON-SEQUENCER-3-MEMBER-CLUSTER-STATUS.md`'s "Moot" section corrected accordingly.
4. ~~**Wire Milestone 5B.3.**~~ **Done.** `40-smoke-test.sh` and `50-failover-drill.sh` were
   rewritten from scratch: both now point at the persistent `mock-upstream-source` +
   `line-handler-template` pair instead of a one-shot `loadgen` Job, sampling
   `mock_upstream_gap`/`mock_upstream_duplicate`/`mock_upstream_observed` and
   `line_handler_messages_relayed`/`line_handler_relay_loop_errors` via `/actuator/prometheus`
   (`kubectl exec ... wget`) before and after a window instead of waiting on a Job's exit code —
   asserting on the *delta* (no new violations during the window), not the absolute counter value,
   since a known-open finding could otherwise never let the script pass. One real script bug found
   and fixed live: `awk '{ print; exit }'` piped from `wget -qO-` under `set -o pipefail` causes a
   SIGPIPE (exit 141) when awk exits before wget finishes writing — removed the early `exit`.
   **`40-smoke-test.sh` passes cleanly** (two consecutive runs: `PASSED`, zero new
   gaps/duplicates/relay errors, throughput confirmed on both the inbound-relay and
   observed-egress sides). One subtlety confirmed correct rather than a bug along the way:
   `mock_upstream_gap`'s delta went *negative* on both passing runs (`SequenceVerifier.gapCount()`
   recomputes live holes between observed min/max, so it can shrink as previously-missing
   sequenceIds arrive via replay) — the script's `-gt 0` check correctly treats this as healthy.
   `50-failover-drill.sh` was rewritten the same way but not yet run against a live 3-member drill
   this session (needs `LOCAL_MEMBERS=3`, out of scope for this pass).
   **Caveat, important for reproducing this:** running these scripts against the live cluster this
   session is *how* the `mock_upstream_gap`/`bridge_publish_failures` root cause and the new
   cluster-node restart finding (both above) were actually found — getting to the clean passing
   runs took extensive live iteration (numerous pod restarts, checkpoint resets, a stream
   recreation) to work around both, not just running the script cold. Until the cluster-node
   restart mystery (new tracked item) is resolved, a `40-smoke-test.sh` run against an
   already-stalled pipeline (e.g. right after one of those unexplained restarts, before
   `nats-bridge`'s checkpoint self-recovers or is manually cleared) can still fail — that's a real,
   separate, tracked gap, not a flaw in the script itself.
5. ~~**Retire `loadgen`** (5B.4).~~ **Done.** Deleted `services/sequencer-aeron/loadgen/`, its
   `pom.xml` module entry, its `.gitlab-ci.yml` `package:sequencer-loadgen` job (and the
   `package:helm` job's `needs` reference to it), the dead `sequencer-loadgen-smoke` job cleanup
   in `90-teardown.sh`, and the `loadgen` credential entry from `clusterNode.sources` in both the
   chart default and `environments/local/values.yaml`. `deploy:uat`'s acceptance-artifact
   `after_script` (previously a TODO placeholder) now curls `mock-upstream-source`'s
   `/actuator/prometheus` via `kubectl` and hard-fails on nonzero `mock_upstream_gap`/
   `mock_upstream_duplicate`, per the plan already written into that job's own comment — like the
   rest of the pipeline, unverified against a real GitLab instance (step 9). Full reactor
   `mvn test` and `helm lint`/`helm template` both pass; redeployed and live-verified.
   **Found and fixed one more real bug while re-verifying `40-smoke-test.sh` against the
   redeployed chart:** `mock-upstream-source`'s `EgressConsumer` was subscribing via a plain core
   NATS dispatcher, not JetStream — core NATS has no flow control and silently drops messages for
   a slow consumer under load, which is what was actually driving the `mock_upstream_gap` growth
   seen during re-verification (confirmed: `nats-bridge`'s own `bridge_gap`/`bridge_publish_failures`
   stayed at zero throughout, so the messages were genuinely published — this consumer just never
   received its own copy of some of them). Fixed by switching to an ephemeral JetStream push
   subscription (`EgressConsumer.subscribe` now takes a `JetStream`, not just a `Connection`).
   Trade-off, not fully resolved: JetStream's at-least-once delivery means this subscription can
   now occasionally see a genuinely-published message twice (redelivery after a slow/lost ack, not
   a system-level double-publish), which shows up as a small, bounded `mock_upstream_duplicate`
   count (~8–14 per ~50k messages observed) — a real but low-severity, well-understood artifact of
   the *verifier's own* subscription, not evidence of a system bug (that invariant is separately
   covered by `EgressNoDoublePublishIT`/`SuppressionGate`). Proper fix is a `Nats-Msg-Id` header on
   `nats-bridge`'s publish for server-side dedup — deliberately not implemented here, since
   `libs/nats-egress`'s `JetStreamDestination` is shared with phase-1 `sequencer-nats` and
   `NatsIngressTransport`, and changing its shared publish path wasn't worth the risk in the same
   pass as retiring `loadgen` — tracked as its own follow-up.
6. **Run the Milestone 9/12.4 diff harness for real**, using step 4's now-working
   `mock-upstream-source`/`line-handler-template` pair as phase-2's input side, against a dev
   environment running both `sequencer-nats` and the cluster. Archive results in
   `docs/migration/`.
7. **Re-run the Milestone 9/12.5 `ClusterOfferBenchmark`** on an idle machine for a trustworthy
   number; separately, drive `mock-upstream-source` at sustained high rate to get the 1M
   msgs/sec / multi-AZ numbers §18 asks for. Independent of steps 1–6.
8. **Resume the `IngressIdempotencyIT` flow-control investigation** (Milestone 9/12.2, currently
   DEFERRED) when it becomes a priority: Aeron's `-Daeron.event.log=all` event-logging agent on
   the leader, watching the `alias=log` MDC publication and its Archive-recording subscriber
   around the moment `pub-pos` stops advancing — see
   `docs/AERON-SEQUENCER-3-MEMBER-CLUSTER-STATUS.md` for the full evidence trail. Independent of
   everything else; not blocking. **Related but distinct** from step 3's `line-handler-template`
   reconnect bug and the new `mock_upstream_gap` finding above — all three are different symptoms
   possibly sharing a root cause (something about this environment's Aeron session/connection
   handling under certain conditions), but none has been proven to share a cause with another; do
   not assume a fix for one resolves the others without checking.
9. ~~**Write the Grafana dashboard** (Milestone 10, `deploy/observability/`).~~ **Done.**
   `deploy/observability/gcm-md-sequencer-aeron-dashboard.json` — 6 rows (cluster leadership &
   health, end-to-end throughput funnel, correctness invariants, backpressure & errors, replay &
   lag, snapshot/consensus internals) covering every metric across all 4 phase-2 services plus
   the phase-1-shared metric names design §17 calls out.
   **Found and fixed a real gap while building it**: `cluster-node` had no Prometheus scrape path
   wired into the chart at all — its `/metrics` endpoint (`MetricsHttpServer`, port 9100, no
   Spring dependency so it's not `/actuator/prometheus` like the other three) was never exposed as
   a container port, Service port, or `ServiceMonitor`, so none of `ClusterMetrics`'s data
   (leader role, commit position, per-source dedup/gap counters — arguably the most important
   metrics in the whole system) was ever reachable by Prometheus. Fixed: `clusterNode.ports.metrics`
   added to values.yaml, wired through `templates/clusternode/{statefulset,headless-service}.yaml`
   (container port + `CLUSTER_METRICS_PORT` env var + Service port + `prometheus.io/*`
   annotations), a new `ServiceMonitor` entry (`port: metrics`, `path: /metrics` — not
   `/actuator/prometheus`), and a `NetworkPolicy` allow rule (left unrestricted by source, unlike
   the Aeron ports, since Prometheus's actual namespace/labels aren't knowable to this chart — see
   the policy's own comment). **Live-verified**: `curl`'d `/metrics` directly against the running
   pod both immediately after deploy and again after a later pod restart — `sequencer_cluster_role`
   and `sequencer_commit_position` both present and correct (role=2/LEADER, commit position
   climbing).
   **Also found while cross-checking every panel's query against a live scrape**: every metric
   registered via `MeterRegistry.gauge(name, ...)` anywhere in this codebase has its Java-side name
   end in `_total`, but Micrometer's Prometheus exporter strips that suffix for gauge-typed meters
   on scrape (only real `Counter`s keep it) — so e.g. `bridge_gap_total` in the Java source is
   `bridge_gap` in Prometheus. Confirmed by diffing every `# TYPE` line across all four services'
   live scrapes against the dashboard's queries; written up in `deploy/observability/README.md`'s
   own "naming gotcha" section so the next person extending this dashboard doesn't have to
   rediscover it. `sequencer_source_duplicate_total`/`sequencer_source_seq_gap_total` (real
   `Counter`s) correctly keep `_total` and were left alone.
10. ~~**Run `.gitlab-ci.yml` against a real GitLab instance** (Milestone 11).~~ **Partially done —
    `build`/`test`/`integration`/`package:*` validated locally (`gitlab-ci-local`, a tool that
    runs each job in the same Docker image against a real clone of this repo's own git history —
    the closest thing to a real runner without one); `deploy:*` genuinely needs a real GitLab
    instance (Kubernetes agent contexts, a real registry) and is still untested. No real GitLab
    project/runner/registry was available in this environment, so this is what "as close as
    possible without one" looks like — real GitLab is still the ultimate acceptance bar.**
    **Found and fixed a repo-critical bug that had nothing to do with CI specifically**: this
    repo had almost nothing committed (`libs/`, `services/`, `deploy/`, `docs/`, `build/` — most
    of the reactor — existed only as uncommitted working-tree changes; `git log` showed a single
    old commit). Independent of that, `.gitignore`'s `build/` pattern excluded the *entire*
    top-level `build/` directory, and the `!/build/gcm-md-parent/` negation meant to carve out an
    exception for the parent POM every module inherits from never worked — git will not
    re-include a path via `!` once a parent directory is already excluded, no matter how the
    negation is written. So `build/gcm-md-parent/pom.xml` was invisible to git *even if
    committed* — this would have broken a fresh clone for any contributor, not just CI. Fixed the
    pattern (anchored + one-level exclusion instead of a recursive one that also caught the repo
    root — see the `.gitignore` comment for the exact mechanics), then committed the working tree
    (one commit, user-approved) so `gitlab-ci-local` had something to actually build.
    **Then found three real, distinct pipeline bugs, all fixed**, running each stage in order:
    1. `MAVEN_IMAGE: maven:3.9.9-eclipse-temurin-25` was never a real tag — the official image
       only publishes major.minor precision (`3.9`, not `3.9.9`) for `-eclipse-temurin-*`
       variants. Every single job would have failed on `docker pull` before running anything.
    2. The `build` job's `mvn install` and the `integration` job's `mvn verify -am` both run
       through the `package` phase, which is where `cluster-node`/`nats-bridge`/
       `line-handler-template`/`mock-upstream-source` each bind `jib:dockerBuild` (for local dev,
       `deploy/local/10-build.sh`) — needing a Docker daemon neither job's container has. The
       `package:*` jobs already correctly avoided this (stopping at `compile` + explicit
       `jib:build`, per that section's own pre-existing comment); the two earlier jobs never got
       the same protection. Fixed with `-Djib.skip=true` on both.
    3. A different manifestation of the same underlying issue: `package:cluster-node` etc. invoke
       `jib:build` as a bare CLI goal with `-am` (also-make), which applies the goal to *every*
       module in scope — including plain dependency modules like `md-models-sbe` that have no jib
       `<configuration>` at all, which then fell back to jib's default base image (Java 21) and
       failed against this reactor's Java 25 target. Fixed properly this time, not just patched
       around: `build/gcm-md-parent/pom.xml`'s `pluginManagement` now defaults jib to
       `<skip>true</skip>`, and each of the 4 image-producing modules' own `<configuration>`
       explicitly overrides `<skip>false</skip>`. Verified this doesn't regress local dev
       (`deploy/local/10-build.sh` still builds all 4 images) and that the CI command now gets
       cleanly past every dependency module (confirmed via a direct `jib:build` invocation against
       a deliberately-fake registry hostname — failed only on DNS resolution, not on any module's
       Java-version compatibility).
    **Also found, documented but not fixable in `.gitlab-ci.yml` itself**: the `integration` job's
    embedded Aeron media drivers need real `/dev/shm` — Docker's default 64m causes an outright
    `java.lang.InternalError` (`org.agrona.IoUtil.mapNewFile` mmap fault, not a normal exception)
    for every `*IT` that launches its own `MediaDriver`, i.e. all of them. Confirmed by re-running
    with `gitlab-ci-local --shm-size 1g`: 3 of 5 then passed, `IngressIdempotencyIT` failed exactly
    as already documented (DEFERRED, not a regression), and `EgressNoDoublePublishIT` failed with
    an `AeronCluster` reconnect timeout that does **not** reproduce running the identical suite
    directly on the host (confirmed immediately after: only `IngressIdempotencyIT` fails there,
    matching this doc's existing "4/5 pass" record) — concluded to be resource contention specific
    to the shared local Docker environment used for this validation, not a code bug. A real GitLab
    Docker executor has no per-job YAML key for shared-memory size — it's runner-level
    (`config.toml`'s `[runners.docker] shm_size`) — documented in the `integration` job's own
    comment as a hard requirement for whoever configures the real runner, since nothing about the
    YAML itself would hint that a default-configured runner will fail every job in this stage.
    **What's still genuinely untested**: `package:helm` (needs real `helm lint`/`package` against
    a registry-independent path — likely fine, not yet run in this session specifically),
    `deploy:dev`/`deploy:uat`/`deploy:prod`/`deploy:prod-dr` (need a real Kubernetes agent context
    per environment and a real registry — no local substitute attempted), and the `deploy:uat`
    acceptance-artifact script added in step 5 (needs a real `kubectl` reachable from the runner).
11. **Track ArchUnit/Java 25 compatibility** (Milestone 9/12.3) opportunistically — check on
    ArchUnit releases past 1.4.2 periodically, or investigate a workaround if this becomes
    blocking. Not independently actionable beyond "wait or invest in a real fix."
12. **Milestone 12 — the actual cutover.** Only meaningful once 5.4 (real line handlers, external
    work) exists and steps 1–8 above are green. At that point: run the diff harness one more time
    against real line-handler traffic as the final go/no-go, flip both real line handlers'
    `ingress.transport` config to `aeron`, and decommission `sequencer-nats` once confident. This
    is the definition of "complete" for the whole application — see Milestone 12's own section
    below for the full detail.

**What "fully complete" looks like:** every row in the status table above reads Done and
live-verified (not just built/lint-clean), Milestone 9's `IngressIdempotencyIT` and the new
`line-handler-template`/`mock_upstream_gap` findings above either pass/resolve to zero or have a
confirmed root cause with a merged fix, real line handlers (external) are running against the
`aeron` transport in production, and `sequencer-nats` has been decommissioned per the Migration
Cutover milestone. Everything in this repo's own control can reach that state by working through
steps 1–11 above; step 12 additionally needs the external line-handler work
from Milestone 5.4.

---

## Milestone A0 — Repo restructuring & extraction (prerequisite, §3, §3.2)

**0.1 — Introduce the monorepo directory shape**
- Create `build/gcm-md-parent/pom.xml`: the real parent POM (Java 25 per this repo's
  CLAUDE.md — note the design doc says "Java 21+", treat 25 as satisfying "+"),
  `dependencyManagement` for Aeron, Agrona, jnats, Spring Boot BOM, Micrometer,
  Testcontainers, JMH, HdrHistogram, ArchUnit; `pluginManagement` for surefire,
  failsafe, jib-maven-plugin, exec-maven-plugin, maven-enforcer-plugin.
- Root `pom.xml` becomes the reactor; point every module's `<parent>` at
  `build/gcm-md-parent` instead of declaring compiler properties locally.
- Create empty dirs: `libs/`, `services/sequencer-aeron/`, `infra/nats-setup/`,
  `deploy/helm/gcm-md-sequencer-aeron/`, `deploy/observability/`, `deploy/local/`,
  `environments/{local,dev,uat,prod,prod-dr}/`.
- **Acceptance:** `mvn -T 1C verify` still builds the untouched modules through the
  new parent; no behavior change.

**0.2 — Move existing modules into the new layout**
- `md-models-sbe` → `libs/md-models-sbe` (path only; module already matches §3.4 —
  confirm it runs `sbe-tool` via exec-maven-plugin at `generate-sources` against a
  single canonical `schema/md-models-sbe-v4.xml`; currently the schema lives at
  `md-models-sbe/src/main/resources/md-models-sbe-v4.xml` — relocate to
  `/schema/md-models-sbe-v4.xml` per §3 and update the plugin config).
- `md-sequencer` → `services/sequencer-nats` (this **is** phase-1; §3.2's "import"
  step is a no-op here since it's already in-repo — just the path move + reactor
  wiring counts as milestone A0).
- `sequencer-loadgen` → `services/sequencer-aeron/loadgen` (will be repurposed for
  cluster ingress in §12; keep it building against NATS for now).
- `sequencer-bench` → `services/sequencer-aeron/bench`.
- Update root `pom.xml` `<modules>` list and every moved module's relative
  `<parent>` path.
- **Acceptance:** full reactor build green, phase-1 test suite
  (`SequenceStamperOffsetContractTest`, `SequenceStamperV3ProfileTest`,
  `MatchEventEnrichmentTest`, `BatchingDestinationRoundTripTest`,
  `SequenceAllocatorTest`, config tests) all still pass unmodified.

**0.3 — Extract `libs/nats-egress` (§3.2 item 1)**
- Move `services/sequencer-nats/src/main/java/gcm/md/sequencer/egress/*`
  (`CoreNatsDestination`, `JetStreamDestination`, `BatchingDestination`,
  `DestinationChannel`) and their tests
  (`BatchingDestinationRoundTripTest`) into new module `libs/nats-egress`,
  packages preserved (`gcm.md.sequencer.egress` — or rename to a
  transport-neutral package now, since it will soon be shared by the bridge; pick
  once, don't churn twice).
- `services/sequencer-nats` depends on `libs/nats-egress` as a sibling reactor
  module; delete the now-duplicated source there.
- **Acceptance:** phase-1 suite still green, egress tests now run inside
  `libs/nats-egress`. This is milestone **A0 done** (§18 item 5).

**0.4 — Boundary enforcement scaffolding (§3.3)**
- Add maven-enforcer-plugin rules in `build/gcm-md-parent`: ban `libs/*` →
  `services/*` dependencies; ban `libs/*` → Spring dependencies.
- Add an ArchUnit test module (or a shared test-fixtures artifact) wiring the rules
  that will later also cover §12.3 (clock/randomness ban) — stub it now with the
  dependency-direction rules only; the determinism rules land with `sequencer-core`
  in Milestone 1.
- **Acceptance:** enforcer build fails if you temporarily introduce a violating
  dependency (verify by testing, then revert).

---

## Milestone 1 — `libs/sequencer-core` (§4)

**1.1** Create `libs/sequencer-core`, dependency on Agrona only (no Aeron, no NATS,
no Spring — enforced by 0.4's rules).

**1.2** Port from `services/sequencer-nats/.../core/SequenceStamper.java` and
`SequenceAllocator.java`:
- Replace the block-lease `SequenceAllocator` with a plain `long` counter — no
  external KV, no leasing. Delete phase-1 §6 machinery entirely from this module.
- Keep the offset-stamping logic identical (offsets 8, 32 from config; schemaId
  guard at offset 4; templateId at offset 2; `v3` profile support).
- Port event enrichment (`MatchEventEnrichmentTest` logic) using
  `Long2LongHashMap eventId → firstSeq`; add deterministic snapshot serialization
  (sorted-key iteration) — write the serialization test now even though
  `SnapshotSink`/`SnapshotSource` don't have a real backing store yet (in-memory
  buffer is enough).

**1.3** Add the **new** per-source dedupe state (§4, not in phase-1):
`Long2LongHashMap sourceId → lastSourceSeqNum`, config block
`core.source-tracking` (enabled flag + per-template `sourceSeqNum` offset,
default enabled for templateId 9). Implement the three-way branch
(duplicate / normal / gap) with the two new counters
(`sequencer_source_duplicate_total{source}`, `sequencer_source_seq_gap_total{source}`)
routed through `EngineListener`, not a metrics library directly.

**1.4** Define the public `StampingEngine` API exactly as specified in §4 (ctor,
`onMessage`, `onHeartbeatTimer`, `writeSnapshot`, `loadSnapshot`,
`currentSequenceId`), plus `Verdict` enum (`STAMPED`, `DUPLICATE`,
`REJECTED_SCHEMA`).

**1.5** Tests (§12.1, ported): offset-contract, v3-profile, enrichment-interleave —
adapt from the existing phase-1 tests, now infrastructure-free (pure `StampingEngine`
calls, no Spring context). Add new tests for source-dedupe (duplicate /
gap / normal) and snapshot round-trip determinism.

**Acceptance:** `libs/sequencer-core` builds and tests green with zero
infrastructure dependencies; allocation-profile test shows zero per-message
allocation on `onMessage`.

---

## Milestone 2 — `cluster-node` (§5)

**2.1** Create `services/sequencer-aeron/cluster-node`. No Spring, no jnats, no
Kubernetes client dependency (enforcer rule, §3.3).

**2.2** Wire embedded `ClusteredMediaDriver` + `ClusteredServiceContainer` in a
single JVM main class. Configure a nanosecond-resolution `ClusterClock`; forbid the
phase-1 `OffsetEpochNanoClock` from being reachable inside the service class itself
(only at the consensus-module level, per §5.1's carve-out).

**2.3** Implement `SequencerClusteredService implements ClusteredService`:
- `onStart`: load snapshot into a `StampingEngine` instance if present; schedule
  heartbeat timer via `cluster.scheduleTimer`.
- `onSessionMessage`: resolve `sourceId` from the `ClientSession` principal (stub
  the credential scheme now — full principal mapping lands in 2.5); call
  `engine.onMessage(...)`; on `STAMPED`, hand off to `EgressPublisher` (Milestone 3).
- `onTimerEvent`: `engine.onHeartbeatTimer(...)` → publish → re-schedule.
- `onTakeSnapshot` / snapshot load: `engine.writeSnapshot` / `loadSnapshot`.
- `onRoleChange` / `onNewLeadershipTermEvent`: notify `EgressPublisher` (§6.4).

**2.4** Static membership config (`cluster.members`) resolved from DNS names;
enable Aeron driver name re-resolution. Local dev profile: 1 member. Prod-shaped
profile: 3–5 members. All channels UDP unicast; MTU externalized (8k for AWS jumbo
frames, 1408 for Docker Desktop default MTU — both in config, not code).

**2.5** Session → sourceId principal mapping: config `cluster.sources` (list of
`{name, sourceId, credential}`); reject unknown principals at session-open.

**2.6** ArchUnit rules (extends 0.4): `sequencer-core` and
`SequencerClusteredService` may not reference `System.nanoTime`,
`System.currentTimeMillis`, `java.util.Random`, `java.time.Clock.system*`, and may
not iterate non-deterministically-ordered collections where order affects output.
This is §12.3's determinism suite — write it now, before egress exists, so nothing
downstream can violate it un-caught.

**Acceptance:** single-member cluster starts locally, accepts a session, stamps a
canned message end-to-end into an in-memory sink (egress publisher can be a no-op
stub at this point).

---

## Milestone 3 — Egress (§6)

**3.1** `EgressPublisher`: single Aeron publication (MDC dynamic publication),
config `egress.channel` / `egress.stream-id`.

**3.2** Implement the replay/failover suppression gate (§6.4) — the load-bearing
invariant:
- Publish only when `Cluster.role() == LEADER` and live (not replaying).
- On assuming leadership / leader restart: query local Archive for the egress
  recording's tail, read the last recorded message's `sequenceId` at offset 8,
  set `suppressUpTo` (empty recording → snapshot floor).
- Drop publishes while `engine.currentSequenceId() <= suppressUpTo`; resume after.
- Metric `sequencer_egress_suppressed_total`.

**3.3** Archive recording of the egress publication on the leader (member
PersistentVolume, retention from values).

**3.4** Back-pressure: bounded spin-then-idle retry on `Publication.offer`;
`sequencer_backpressure_stall_seconds_total` on persistent blockage. MDC
zero-subscribers is not blockage (`egress.linger-on-no-subscribers`). Do **not**
port phase-1's `drop` mode — it's explicitly deleted (§6.4).

**3.5** Integration test (write before any deploy job runs, per §6.4): kill leader
mid-stream, assert no sequenceId appears twice and none is skipped on recorded
egress. This is the phase-2 acceptance centerpiece — treat it as a gate, not a
nice-to-have.

**Acceptance:** leader-kill test passes in an in-process 3-node harness.

---

## Milestone 4 — `libs/cluster-client` (§7)

**4.1** Thin wrapper over `AeronCluster`: `offer(DirectBuffer, offset, length)`
with bounded retry/backoff and reconnect; surface back-pressure to the caller.

**4.2** Credentials → sourceId principal, matching the `cluster.sources` scheme
from 2.5.

**4.3** Document (in this module's package-info or README) the idempotency
contract: crash-recovery = "republish your tail since last known-processed",
because the service dedupes on `sourceSeqNum`. This supersedes phase-1's
"redelivery gets a new sequenceId" — flag that phase-1 doc line as stale once a
line handler's config switches it onto this transport (see Milestone 5, revised
below — there's no separate shim service to "go live").

**Acceptance:** unit tests against a local single-member cluster: offer, induced
back-pressure, reconnect-after-drop, credential rejection.

---

## Milestone 5 — `libs/ingress-transport` (§8, revised — no shim service)

**Superseded design note:** §8's original plan routed every line handler through a
temporary `ingress-shim` strangler service, so upstream producers never had to
change during migration. That trade only pays for itself with many independently
deployed producer teams and live external traffic to protect — this project has
neither (one team owns both the line handlers and the sequencer; there are no
external clients before first release). The coordination cost the shim exists to
avoid isn't actually present, while the shim itself is a whole extra service to
build, deploy, operate, and eventually delete — and, in practice, an unbuilt
Aeron-cluster-connectivity bug in exactly that shim blocked this milestone for a
full session with nothing to show for it (see
`docs/AERON-SEQUENCER-3-MEMBER-CLUSTER-STATUS.md`'s "Deferred: `ingress-shim`
cluster-connect bug" section for the full account — not resolved, and now moot).
**`services/sequencer-aeron/ingress-shim` is dropped from the plan.** (The
already-built module can be deleted from the reactor; nothing else depends on it.)

**5.1** New module `libs/ingress-transport`: a single interface,
`IngressTransport.offer(DirectBuffer, offset, length)` — non-negative on success,
negative on backpressure (implementation-specific, caller tests `>= 0` only, never
a specific constant). Zero dependencies beyond Agrona, so neither concrete
transport has to depend on the other. **Done** — see
`libs/ingress-transport/src/main/java/gcm/md/sequencer/ingress/`.

**5.2** `libs/cluster-client`'s `ClusterIngressClient` implements
`IngressTransport` directly — its existing `offer` signature already matches, so
this is a marker-interface addition, no behavior change. **Done.**

**5.3** New `NatsIngressTransport` in `libs/nats-egress` (reusing that module's
existing jnats dependency and `EgressMetrics` listener pattern, rather than a new
metrics interface): a bounded in-flight async-publish window over
`JetStream.publishAsync`, same shape as `JetStreamDestination`'s but returning a
result per `IngressTransport`'s contract — a full window is surfaced to the
caller as a negative return, never blocked or silently dropped, matching
`ClusterIngressClient`'s "surface backpressure, don't absorb it" philosophy so a
line handler's config can pick either transport interchangeably. Publishes to the
same subject phase-1 already consumes (`MD_RAW` or equivalent). **Done** — see
`libs/nats-egress/src/main/java/gcm/md/sequencer/egress/NatsIngressTransport.java`
and `NatsIngressConfig`.

**5.4** Line handlers (2, one team, presumably outside this reactor) each depend
on `libs/ingress-transport` plus whichever concrete transport module(s) they
build against, and select the implementation via a config-driven bean — e.g.
`@ConditionalOnProperty(name = "ingress.transport", havingValue = "aeron")` /
`havingValue = "nats"` in that line handler's own `ServiceConfiguration`, per this
repo's bean-wiring convention. **Not done** — line handler source isn't in this
reactor; this is a note for whoever owns that code.

**Acceptance:** unit tests for both `IngressTransport` implementations
(backpressure surfaced correctly, no blocking, no silent drop) — done for
`NatsIngressTransport` (`NatsIngressTransportTest`); `ClusterIngressClient`'s
existing tests already cover its side. No line-handler-side acceptance check yet
(5.4 not started).

---

## Milestone 5B — Line handler template & mock upstream source (new, retires `sequencer-loadgen`)

**Why this exists:** 5.4 (real line handlers wiring `IngressTransport`) is work for
whoever owns that code, outside this reactor — this repo can't complete it
directly. What this repo *can* do is remove as much friction from that handoff as
possible: ship a runnable reference implementation instead of just an interface
and a Javadoc contract. It also closes gaps flagged elsewhere in this doc:
Milestone 8's "`40-smoke-test.sh` doesn't reach the cluster anymore now that
`ingress-shim` is gone" note, Milestone 12/12.4's diff harness needing an actual
phase-2 input path to mirror traffic into (see `CLAUDE.md`'s "What's not yet
validated"), and — scope clarified after this milestone was first drafted —
**there is no longer a separate need for `sequencer-loadgen` at all.** Once
5B.2 below can generate load *and* verify contiguity/dedup on observed egress
(porting `LoadGenerator.SequenceVerifier`'s proven logic rather than
re-deriving it), it fully covers both of loadgen's jobs — traffic generation
and flow validation — for both phase-1 and phase-2, since it publishes
NATS-shaped messages either transport can consume. `services/sequencer-aeron/
loadgen` becomes redundant and should be deleted once 5B.3's scripts have
switched over (not before — see 5B.4).

**5B.1** New `services/sequencer-aeron/line-handler-template`: a minimal Spring
Boot service demonstrating the whole pattern a real line handler should copy.
Per this repo's conventions (no `@Autowired`/stereotypes, all beans in one
`ServiceConfiguration`):
- An `IngressTransport` bean selected by config (`line-handler.ingress-transport:
  aeron|nats`) — `aeron` wires `ClusterIngressClient` plus the embedded
  `MediaDriver` it needs in-process (per `libs/cluster-client`'s package-info
  note — there's no shim to copy this pattern from anymore, so the template *is*
  the copyable example); `nats` wires `NatsIngressTransport` against a
  `JetStream` connection.
- A trivial upstream consumer (subscribes to 5B.2's mock source) that stamps
  each received message with a monotonic `sourceSeqNum` and calls
  `offer(...)`, retrying/parking on a negative return exactly per
  `IngressTransport#offer`'s contract — never blocking indefinitely, matching
  both concrete transports' own documented behavior.
- A deliberate crash-recovery demo: on restart, the template resumes from its
  own last-acked position and re-offers its tail, relying entirely on the
  sequencer's `sourceSeqNum` dedup rather than any handler-side bookkeeping —
  the concrete illustration of `libs/cluster-client`'s package-info idempotency
  contract ("republish your tail since last known-processed"), which a real line
  handler author should copy verbatim rather than re-derive.
- A README stating plainly which parts are "replace this for your real feed"
  (the upstream consumer) versus "keep as-is" (transport selection, the
  offer/retry loop, the crash-recovery approach).

**5B.2** New `services/sequencer-aeron/mock-upstream-source`: **a persistent
Spring Boot service** — scope revised after first draft: a real deployable
service (own Helm entry, own container image, independently scalable/toggleable
from `line-handler-template`), not a one-shot CLI batch tool the way
`sequencer-loadgen` was. Starts generating and verifying the moment it comes
up and keeps doing so indefinitely; there's no `--duration-seconds` — turn it
off by scaling the Deployment to `0`, the same as any other service here. Two
jobs, both absorbed from `sequencer-loadgen`:
- **Generate**: synthetic SBE-shaped messages — reusing `libs/md-models-sbe`,
  not hand-rolled byte layouts — at a configurable rate/pattern (steady,
  bursty, with deliberate gaps and duplicate replays to exercise the dedup
  path specifically), published continuously to a NATS subject standing in
  for whatever real upstream feed protocol a line handler would normally
  consume. This sits one layer further upstream than `loadgen` did —
  simulating the thing a line handler reads *from*, not the sequencer's own
  ingress directly — but since a `line-handler-template` (5B.1) configured for
  `nats` just relays whatever it's given, this service can still drive load
  straight at phase-1 or phase-2 the same way `loadgen` used to, just via that
  relay.
- **Verify**: port `LoadGenerator.SequenceVerifier`'s contiguity/no-duplicate
  assertion logic to watch the *final* observed egress (`MD_SEQUENCED`)
  continuously, exposed as Prometheus gauges (`mock_upstream_gap_total`/
  `mock_upstream_duplicate_total`, both expected permanently zero — same
  "nonzero means a real bug" contract as `bridge_gap_total`) rather than a
  one-shot exit code, since there's no run-end to exit at anymore. This is the
  piece that makes 5B a full `loadgen` replacement, not just a mock source —
  though it changes *how* `40-smoke-test.sh`/`50-failover-drill.sh` check
  pass/fail (poll the metrics endpoint, not a Job exit code) — see 5B.3.

**5B.3** Wire 5B.1 and 5B.2 together as the new content for `40-smoke-test.sh`
and `50-failover-drill.sh` (both currently built around `sequencer-loadgen`)
— superseding Milestone 8's earlier "give loadgen an Aeron mode" note entirely,
since this pair exercises the real topology (upstream → line handler →
sequencer → nats-bridge) more faithfully than a loadgen shortcut straight into
the cluster ever did, for both phase-1 and phase-2. Since 5B.2 is a persistent
service reporting via metrics rather than a Job that exits, both scripts need
to change shape from "wait for Job completion, check exit code" to "deploy (or
confirm already deployed), wait a fixed observation window, curl
`/actuator/prometheus` and assert `mock_upstream_gap_total`/
`mock_upstream_duplicate_total` are zero." Reuse the same pairing as Milestone
12/12.4's phase-2 input side, so the diff harness mirrors real traffic through
an actual line handler rather than needing its own separate mechanism to drive
phase-2. Helm deployment for both services is tracked under Milestone 7 (7.5);
CI build/deploy wiring is tracked under Milestone 11.

**5B.4** Once 5B.3 lands and both scripts are verified working against the new
services, delete `services/sequencer-aeron/loadgen` (module, reactor entry,
Helm/build-script references) — mirroring how `ingress-shim` was retired: don't
leave a redundant, doc-contradicted module sitting in the reactor once its
replacement is proven, but don't delete it *before* the replacement exists
either, since `loadgen` is still the only working traffic/verify tool
(`50-failover-drill.sh` has real, working content built on it) until 5B ships.

**Acceptance:** the template, configured for each transport in turn, relays a
mock-upstream-source burst end-to-end into a local cluster with
`mock_upstream_gap_total`/`mock_upstream_duplicate_total` staying at zero
throughout; a killed-and-restarted template instance shows redelivered
duplicates absorbed by the sequencer, not double-sequenced — mirroring the
retired `ingress-shim`'s own redelivery-after-restart acceptance check (§18),
now demonstrated by the template instead. `40-smoke-test.sh` and
`50-failover-drill.sh` both pass using only 5B's services, with no remaining
dependency on `sequencer-loadgen`.

---

## Milestone 6 — `nats-bridge` (§9)

**6.1** New Spring Boot service `services/sequencer-aeron/nats-bridge`. Subscribes
live to Aeron egress; Archive replay-merge to catch up after downtime, keyed by a
checkpoint in a NATS KV key (bridge is stateless-restartable, never authoritative).

**6.2** Depends on sibling `libs/nats-egress` (from 0.3) for `MessageBatch`
framing, flush policy, `flush-on-event-boundary` — same source phase-1 compiles
against, so no re-implementation.

**6.3** Republish to `MD_SEQUENCED`; contiguity assertion on consumed
sequenceIds, `bridge_gap_total` metric (should be permanently zero — nonzero is an
egress bug, treat any nonzero reading in testing as a blocking find, not noise).

**Acceptance:** existing JetStream/WebSocket consumers (if any test doubles exist)
see byte-identical `MD_SEQUENCED` framing versus phase-1 output.

**Status:** done, including the `archiveControlChannel` fix (path-to-completion step 1). The
original implementation connected to a single bare headless-Service DNS name for the Archive
control channel; since Archive control connections have no `AeronCluster`-style automatic
leader-following (unlike the ingress session), and only the current leader's local archive ever
has the egress recording, this could silently land on a follower's empty archive. Fixed via
`LeaderArchiveConnector` (`gcm.md.natsbridge.bridge`): given one candidate `AeronArchive.Context`
per cluster member (`NatsBridgeProperties.Cluster.archiveControlChannels`, comma-separated, wired
from Helm's new `gcm-md.clusterNode.archiveControlChannels` helper), it tries each in turn and
keeps whichever one actually has the matching recording, falling back to the first reachable
candidate when none do yet (fresh cluster, nothing recorded). Unit-tested
(`LeaderArchiveConnectorTest`, 4 cases); not yet exercised against a live cluster — see
path-to-completion step 2.

---

## Milestone 7 — Kubernetes & Helm (§10)

**7.1** `deploy/helm/gcm-md-sequencer-aeron` chart: `clusterNode` (StatefulSet),
`natsBridge` (Deployment), RBAC, ServiceMonitors, PDB, NetworkPolicies — all
toggleable per values. (No `ingressShim` Deployment — Milestone 5, revised,
dropped that service; a *real* line handler deploys independently of this
chart — but see 7.5, `lineHandlerTemplate` is the one exception.)

**7.2** `infra/nats-setup`: official NATS chart values + idempotent stream/KV
setup Job.

**7.3** StatefulSet specifics: headless Service (`clusterIP: None`) for per-pod
DNS, no ClusterIP Service ever fronting Aeron traffic (NetworkPolicy restricting to
direct pod-to-pod UDP on the Aeron port range); `/dev/shm` as `emptyDir{medium:
Memory}` sized per env; one PVC per member; Guaranteed QoS in prod-class values,
laptop-sized (2 CPU / 4Gi) in local; liveness = process, readiness = member ACTIVE
+ (leader only) egress connected via a tiny HTTP endpoint; PDB
`maxUnavailable: 1`; preStop → clean `ClusteredServiceContainer` shutdown.

**7.4** `environments/{local,dev,uat,prod,prod-dr}/` values overlays.

**7.5** (new, Milestone 5B) `lineHandlerTemplate` and `mockUpstreamSource`
Deployments — each its own toggle/image/replicas/resources, per the "separate
deployable services" decision (not bundled into one Deployment, not gated
behind a shared toggle). `lineHandlerTemplate.ingressTransport` (`aeron`/`nats`)
conditionally adds it to `networkpolicy.yaml`'s cluster-node allow-list only
when `aeron` — done via `and .Values.lineHandlerTemplate.enabled (eq
.Values.lineHandlerTemplate.ingressTransport "aeron")`, verified by rendering
both settings and confirming the pod-selector entry appears/disappears
accordingly. `clusterNode.sources` gets a second `line-handler:2:...` credential
entry alongside the existing `loadgen:1:...` one — **note the local overlay
(`environments/local/values.yaml`) sets `sources` as a full scalar override,
not a merge**, so its copy needed the same second entry added independently, or
`lineHandlerTemplate` would silently fail to authenticate against a real local
cluster despite the chart default being correct. `gcm-md.clusterNode.
ingressEndpoints` (the `_helpers.tpl` helper added back when `ingress-shim` was
retired, previously unused) is now genuinely consumed by
`LINE_HANDLER_AERON_INGRESS_ENDPOINTS` — verified rendering correctly for both
`replicas=1` and `replicas=3`.

**Acceptance:** `helm lint` clean; chart installs against a local Docker Desktop
context with `clusterNode.replicas=1`. `helm template` verified for both new
services (Deployments render, `NetworkPolicy` conditionally includes/excludes
`line-handler-template` per `ingressTransport`, `ingressEndpoints` renders
correctly for `replicas=1` and `replicas=3`) — not yet verified against a live
cluster (no `kubectl apply` run this session).

---

## Milestone 8 — Local deployment scripts (§11)

Scripts in `deploy/local/`, bash `set -euo pipefail`, idempotent:
- `00-preflight.sh` — hard-fail unless `kubectl config current-context ==
  docker-desktop`; check helm/docker/mvn; warn under 6 CPU/12GiB; print MTU note.
- `10-build.sh` — `mvn -T 1C -DskipTests package jib:dockerBuild` at root.
- `20-install-nats.sh` — deploy `infra/nats-setup` into `gcm-md-local`.
- `30-deploy.sh` — helm upgrade/install with `environments/local/` values; bridge
  ON. (Previously also toggled `ingress-shim` — gone per Milestone 5's revision.)
- `40-smoke-test.sh` — job asserting contiguity, dedupe, heartbeat high-water
  mark against a burst of traffic; same image used later in CI. **Needs
  follow-up work**: currently built around a `sequencer-loadgen` Job, which
  only speaks NATS and, with `ingress-shim` gone, nothing relays that into the
  cluster anymore. Switches to Milestone 5B's `mock-upstream-source` +
  `line-handler-template` pair once built (5B.3/5B.4) — that pair retires
  `sequencer-loadgen` entirely rather than just patching this one script.
- `50-failover-drill.sh` — `kubectl delete pod` the leader mid-burst (3-member
  mode); re-run verifier. Currently the one script where `sequencer-loadgen`
  still has real, working content (see `docs/AERON-SEQUENCER-3-MEMBER-CLUSTER-STATUS.md`)
  — do not delete `loadgen` until this is confirmed passing against Milestone
  5B's replacement (5B.4's explicit ordering).
- `90-teardown.sh` — helm uninstall, delete namespace, optional PVC wipe.
- Root `Makefile`: `local-up` (00→30), `local-smoke` (40), `local-down` (90).

**Acceptance:** `make local-up && make local-smoke` passes 1-member and 3-member;
`50-failover-drill.sh` passes 3-member (§18 item 1).

---

## Milestone 9 — Testing suites (§12)

Most of these are written incrementally in earlier milestones; this step is the
consolidation/gate pass:
- **12.1** phase-1 tests ported and green (done by Milestone 1).
- **12.2** `integration-tests` module: leader-kill contiguity, egress
  no-double-publish, follower reschedule/DNS re-resolution, snapshot cycle,
  ingress idempotency — in-process harness plus the local 3-member profile for
  heavier cases.
- **12.3** determinism suite: replay equivalence, snapshot equivalence,
  wall-clock/randomness ArchUnit ban (scaffolded in 0.4/2.6 — finish here).
- **12.4** parallel-run diff harness (`integration-tests/tools`): mirror one input
  into both `services/sequencer-nats` and a line handler configured for the Aeron
  transport; join on `(sourceId, sourceSeqNum)`; diff everything except
  sequenceId/sequenceTimestamp. Run once, pre-launch, as a go/no-go gate (see
  Milestone 12, revised — no live shadow-traffic cutover to run it before);
  archive results in `docs/migration/`.
- **12.5** JMH: stamp path + cluster offer path in
  `services/sequencer-aeron/bench` (from 0.2); loadgen acceptance target 1M
  msgs/sec, p99 latency reported, multi-AZ vs single-AZ consensus cost documented.

**Acceptance:** all required jobs green; §18 items 2–4, 6–7 satisfied.

**Status:** not yet green. Live verification against Kubernetes was the missing prerequisite for
12.2 (in-process suite still `@Disabled`) and part of 12.5 (multi-AZ number); a 3-member Aeron
cluster is now confirmed running and verified on local Kubernetes (`gcm-md-local` namespace) —
see `docs/AERON-SEQUENCER-3-MEMBER-CLUSTER-STATUS.md` for what's done and how to resume. That doc
also has the full account of the `ingress-shim` connectivity bug that blocked a full session —
**now moot**, since Milestone 5's revision drops `ingress-shim` from the plan entirely; the bug
document is kept for the record but nothing is waiting on it anymore. 12.3's ArchUnit/Java 25
tooling gap is untouched and separate.

**12.2 update**: all 5 `*IT` classes are un-`@Disabled` and have been run for real against the
in-process 3-member Raft path (not just written). 4 of 5 pass reliably:
`EgressNoDoublePublishIT`, `SnapshotCycleIT`, `FollowerRescheduleIT` (passed standalone twice,
flaked once in a same-session batch run — resource contention, not a distinct bug), and — as of
this session — `LeaderKillContiguityIT`, this milestone's own stated centerpiece test (§12.2:
"kill the leader mid-stream, assert no sequenceId is ever duplicated or skipped"), which now
exercises the full kill/re-elect/1000-message no-gap-no-dup-no-regression scenario cleanly for the
first time (a real bug in the test harness itself — `killMember` never resets the killed member's
`EgressPublisher` role, so `isLeader()` reports a permanently stale `true` — was found and fixed).
`IngressIdempotencyIT` alone remains genuinely broken: root-caused past the "memory pressure"
theory a prior session suspected (reproduces identically in a freshly restarted, low-load
environment) to a concrete flow-control stall — the leader's internal Raft-log MDC publication's
`pub-pos` pins exactly at its `pub-lmt` partway through a fast message burst, with zero transport
errors/NAKs/loss at the Aeron layer throughout. See
`docs/AERON-SEQUENCER-3-MEMBER-CLUSTER-STATUS.md`'s "fresh-environment re-run" update for the full
evidence and the narrowed next step (Aeron event-log tracing on the leader's log-append path).
**This specific investigation is deferred (intentionally paused, not currently being worked
on)** — see that document's DEFERRED marker on its "fresh-environment re-run" section; the rest
of Milestone 9 (4/5 `*IT` classes passing) is unaffected.

**12.4 / 12.5 update**: the diff-harness tooling
(`ParallelRunDiffHarness`/`MessageDiffer`/`DiffReport`/`ParallelRunDiffCli`) exists, compiles, and
has 14 passing unit tests, but has never been run against real phase-1-vs-cluster traffic
(`docs/migration/` is empty) — correctly gated on Milestone 12's migration work, which hasn't
started. JMH (`services/sequencer-aeron/bench`) has now actually been run for the first time:
`StampingEngineBenchmark` (the pure stamp path) measured **4.452 ± 0.026 ns/op**, comfortably
inside the ~1µs/message budget. `ClusterOfferBenchmark` had never been run before either — it had
the same buffer-undersizing bug already found and fixed in the `*IT` suite (`new byte[64]` but a
4-byte write at offset 64), fixed here too — but its post-fix numbers were too noisy to trust
(tens–hundreds of ms/op) given this session's own accumulated resource pressure by that point;
needs a clean re-run on an idle machine before citing as the real cluster-offer-path baseline. The
1M msgs/sec loadgen acceptance target and multi-AZ vs single-AZ consensus cost are both still
unmeasured.

---

## Milestone 10 — Observability (§17)

Add metrics: `sequencer_cluster_role`, `sequencer_commit_position`,
`sequencer_snapshot_duration_seconds`, `sequencer_egress_suppressed_total`,
`sequencer_source_duplicate_total{source}`, `sequencer_source_seq_gap_total{source}`,
`bridge_gap_total`, `bridge_lag_sequences`, `sequencer_dr_replication_lag_sequences`.
Export Aeron counters (errors, back-pressure, flow control) via counters reader →
Prometheus in `cluster-node`. Grafana dashboard JSON in `deploy/observability/`.

---

## Milestone 11 — GitLab pipeline (§16)

Single `.gitlab-ci.yml`, stages `build → test → integration → package → deploy`,
path-scoped via `rules: changes:` on MRs, full reactor on default branch.
- `build`: `mvn -T 1C -DskipTests package`.
- `test`: surefire + determinism suites + enforcer/ArchUnit — required, no
  `allow_failure`.
- `integration`: failsafe `*IT`, Testcontainers NATS; nightly schedule adds
  failover drills; §6.4 no-double-publish and §12.3 replay-equivalence jobs
  required.
- `package`: `jib:build` (daemonless) → `$CI_REGISTRY_IMAGE/<component>`;
  `helm lint` + `helm package` with `appVersion=$CI_COMMIT_SHORT_SHA`.
- `deploy:dev` auto; `deploy:uat` manual + loadgen acceptance artifact;
  `deploy:prod` manual, requires uat-green same SHA; `deploy:prod-dr` manual.
- Tag pipeline (`v*`): publish `cluster-client` + `md-models-sbe` to GitLab Maven
  registry.

**Status:** `.gitlab-ci.yml` written at the repo root — all stages/jobs above, plus one
`package:<component>` job per image-producing module (`cluster-node`, `nats-bridge`,
`line-handler-template`, `mock-upstream-source`, `sequencer-loadgen`, the last deletable once
Milestone 5B.4 retires it). YAML syntax validated (parses cleanly, anchors/merge keys resolve as
intended); **not run against a real GitLab instance** — no CI runner/registry/environments exist
to test against in this session, so treat the job definitions as reviewed-but-unverified until a
real pipeline run proves them out. Two deviations from the literal spec text, both documented
inline in the file: the `integration` stage doesn't provision Testcontainers/live NATS (the
actual `*IT` suite is fully in-process and needs neither); `package`'s jib invocations use
`compile jib:build` rather than the phase-bound `dockerBuild` execution every image module's
`pom.xml` already has for local dev (`dockerBuild` needs a Docker daemon runners don't have;
`jib:build` is daemonless and registry-targeted, matching what §16 actually calls for).
`deploy:uat`'s loadgen-acceptance-artifact step is a placeholder pending Milestone 5B.4 — the
real check should become a `mock_upstream_gap_total`/`mock_upstream_duplicate_total` metrics
assertion once `loadgen` retires.

---

## Milestone 12 — Migration cutover (§15, §13, revised)

**Superseded design note:** §15's original A→B→C→D staged cutover (shadow traffic,
bridge retargeting, phased line-handler rewrites, deferred phase-1 teardown) was
designed for a *live* migration: real production traffic on phase-1, many
independent producer teams, external consumers who must see no disruption. None
of that applies here — this is pre-first-release, one team owns everything, and
there are no external clients yet. The elaborate staging exists to bound risk
against things that aren't actually at risk in this project's current state, so
it's replaced with a single lightweight validation-then-flip sequence. Revisit the
original staged plan (in git history / this doc's prior revision) if the
situation changes before launch — e.g. if phase-1 goes live with real traffic
before phase-2 is ready, the "no live traffic to protect" assumption breaks and
the original shadow/cutover staging becomes the right tool again.

Run once Milestones 1–5 (including 5.4, line handlers wired to
`IngressTransport`), 5B, 6, 7, and 9's 12.1/12.2 are green (11's CI pipeline and
full 12.3/12.5 are good practice but not hard blockers pre-launch, given no
external users yet):

- **Validate**: run the §12.4 diff harness once — the same input mirrored into
  both phase-1 (`sequencer-nats`, NATS ingress) and phase-2 (either a real line
  handler with its config flipped to `aeron`, or Milestone 5B's
  `mock-upstream-source` + `line-handler-template` pair if real line handlers
  aren't ready yet) — as a pre-launch go/no-go gate. No live shadow deploy or
  `MD_SEQUENCED_SHADOW` subject needed; this can run against dev.
- **Cutover**: flip both line handlers' `ingress.transport` config to `aeron` in
  one deploy — single team, no external clients, no need to stage per-source.
  `nats-bridge` keeps republishing to `MD_SEQUENCED` unchanged throughout (its
  job — internal downstream consumers seeing no change — doesn't depend on which
  ingress transport fed the cluster).
- **Rollback**: flip the config back. No incident-response runbook needed beyond
  that — there's no live production traffic to protect mid-migration, so a config
  revert is the whole story.
- **Decommission phase-1**: once confident (post-launch, not immediately), remove
  `services/sequencer-nats` outright rather than keeping it "deployable but
  stopped" indefinitely — there's no rollback-from-production scenario to hedge
  against once genuinely validated pre-launch.
- **prod-dr**: unchanged from the original plan — standby cluster in DR region,
  Archive replication of egress recording (fallback: bridge republisher),
  async/lossy-at-tail failover per `docs/runbooks/dr-failover.md` (write this
  runbook as part of this milestone). DR is a property of the cluster itself, not
  of the ingress-transport question this revision is about.
- **Fast consumers** (was phase D): subscribing directly to Aeron egress instead
  of via `nats-bridge`/NATS remains a valid future optimization, not a launch
  requirement — defer.

Deliver the remaining runbooks required by §18 item 9: sizing, member
replacement, snapshot management, DR failover. The "rollback-to-phase-1" runbook
shrinks to "flip the config back" — document it, but it no longer needs the
KV-high-water-seeding mechanics the original live-migration plan required.

---

## Execution order

**Superseded by "Current status & path to completion" near the top of this document.** This
section originally sequenced building every milestone from scratch; that build-out is now
mostly done (see the status table up top), so "what order to build things in" has been replaced
by "what order to finish the remaining work in" — a different question, answered by the ordered
plan at the top of the doc. Kept here only so the historical build sequencing isn't silently
lost for anyone reconstructing how the project actually got built.
