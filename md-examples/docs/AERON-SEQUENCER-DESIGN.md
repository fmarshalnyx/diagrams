# GCM-MD Platform — Phase 2 Design & Implementation Specification (Aeron Cluster Sequencer)

Input specification for Claude Code. This document is authoritative for phase 2 and is
written in the same style as `SEQUENCER-PROJECT.md` (phase 1), which remains the
authority for the message schema contract, stamping offsets, and hot-path discipline.
Where this document says "config", the setting must appear in the stated file with the
exact key shown. Where phase-1 rules are not explicitly overridden here, they carry
forward.

Repository model: **single monorepo, single Maven reactor** (§3). One commit SHA is
the complete bill of materials for source, images, charts, and environment config.
Build system: **Maven**, Java 21+.

Companion documents (in `docs/` of the monorepo):
- `SEQUENCER-PROJECT.md` — phase-1 sequencer spec (schema contract §3 is still law)
- `md-models-sbe-v4.xml` — canonical SBE schema (schemaId=100, v4, little-endian)

---

## 1. Purpose and scope

Replace the phase-1 NATS-based sequencer's ordering/HA machinery with an
**Aeron Cluster** (Raft) whose clustered service performs the sequence stamping.
The cluster's replicated log becomes the ordering and durability backbone;
NATS/JetStream remains for replay and WebSocket distribution via a bridge, and is
removed from the critical ingress path at the end of the migration.

What the cluster design buys over phase 1:
- **Gap-free, contiguous sequenceIds** — block leasing (phase-1 §6) is deleted; the
  counter is deterministic replicated state, reconstructed exactly on failover/restart.
- **No external leader election** — Raft replaces the Kubernetes Lease + fabric8
  machinery and the KV fencing checkpoints (phase-1 §10).
- **Lossless leader failover** — acknowledged ingress survives leader death; no
  warm-standby choreography.
- **Deterministic replay** — the log + snapshots are the audit trail and the recovery
  mechanism.

Out of scope: line-handler internals (they consume `libs/cluster-client`), per-symbol
fan-out, the replay/gap-request service (JetStream egress remains its data source),
colo deployment (design for portability only, §14).

## 2. Non-negotiable requirements

1. sequenceId is **monotonic, sequential, and contiguous** across all messages, all
   instruments, all sources, including across leader failover, restart, and
   snapshot-restore. (Stronger than phase 1: gaps are no longer permitted in steady
   operation; the only tolerated discontinuity is a documented DR-region failover, §13.)
2. The schema contract of phase-1 §3 is unchanged: stamp u64 LE at absolute offset 8
   (`sequenceId`) and 32 (`sequenceTimestamp`); MatchEventBoundary (templateId=6)
   enrichment at 56/64 when enabled; offsets externalized in config, never hardcoded;
   schemaId sanity guard at offset 4. The stamper never decodes SBE.
3. The clustered service is **strictly deterministic**: no wall clocks, no
   `System.nanoTime`/`currentTimeMillis`, no randomness, no iteration over
   non-deterministically-ordered collections when order affects output, no reads of
   environment/filesystem/network. Time comes only from the cluster clock. Enforced by
   tests and ArchUnit rules (§12.3), not convention.
4. No sequenced message is ever published twice on the egress stream (replay/failover
   suppression, §6.4), and no acknowledged ingress message is ever silently dropped.
5. Per-message work in the service remains allocation-free in steady state (Agrona
   buffers, primitive maps, reused scratch buffers).
6. Everything is deployable from the GitLab pipeline, and the full stack must come up
   on a developer laptop against Docker Desktop Kubernetes with a single command from
   a fresh `git clone`.
7. Module boundaries are enforced mechanically (§3.3): `libs/` never depends on
   `services/`; hot-path modules never depend on Spring or SBE codecs at runtime.

## 3. Repository — monorepo layout and Maven structure

One repository: **`gcm-md-platform`**. One Maven reactor rooted at `/pom.xml`.
"What is deployed to environment X" is answered by a single commit SHA; environment
promotion is an MR (or tag) in this repo — nothing else.

```
gcm-md-platform/
├── pom.xml                          # reactor root; inherits build/gcm-md-parent
├── .gitlab-ci.yml                   # single pipeline, path-scoped jobs (§16)
├── Makefile                         # make local-up / local-smoke / local-down
├── docs/                            # this spec, phase-1 spec, runbooks/, migration/
├── schema/md-models-sbe-v4.xml      # canonical schema (single copy, referenced by build)
├── build/
│   └── gcm-md-parent/               # parent POM: dependencyManagement + pluginManagement
├── libs/
│   ├── md-models-sbe/               # SBE codegen artifact (§3.4)
│   ├── sequencer-core/              # deterministic stamping engine (§4)
│   ├── nats-egress/                 # EXTRACTED from phase-1: DestinationChannel,
│   │                                #   CoreNats/JetStream/BatchingDestination,
│   │                                #   MessageBatch writer (+ their tests)
│   └── cluster-client/              # Aeron Cluster ingress client (§7)
├── services/
│   ├── sequencer-nats/              # phase-1 sequencer, imported (§3.2)
│   └── sequencer-aeron/
│       ├── cluster-node/            # §5–6 — Aeron Cluster member image
│       ├── ingress-shim/            # §8 — MD_RAW -> cluster (strangler; temporary)
│       ├── nats-bridge/             # §9 — Aeron egress -> MD_SEQUENCED
│       ├── loadgen/                 # §12 — canned-SBE load generator
│       ├── integration-tests/       # §12 — failover/determinism/diff suites (*IT)
│       └── bench/                   # JMH: stamp path, offer path
├── infra/
│   └── nats-setup/                  # NATS chart values + stream/KV setup Job image
├── deploy/
│   ├── helm/gcm-md-sequencer-aeron/ # chart (clusterNode, ingressShim, natsBridge)
│   ├── observability/               # Grafana dashboards, alert rules
│   └── local/                       # Docker Desktop scripts (§11)
└── environments/
    ├── local/  dev/  uat/  prod/  prod-dr/    # Helm values overlays per env
```

### 3.1 Maven conventions
- Every module inherits `build/gcm-md-parent`: Java 21, encoding, dependencyManagement
  (Aeron, Agrona, jnats, Spring Boot BOM import, Micrometer, Testcontainers, JMH,
  HdrHistogram, ArchUnit), pluginManagement (surefire for unit tests, failsafe for
  `*IT`, jib-maven-plugin, exec-maven-plugin for SBE, maven-enforcer-plugin).
- **Intra-repo dependencies are sibling reactor modules** — no publishing, no version
  pinning, no registry between components. The whole tree carries one
  `${revision}`-style version; nothing is consumed outside this repo in phase 2 (if
  line handlers live in another repo at migration phase C, publish `cluster-client` +
  `md-models-sbe` to the GitLab Maven registry from a tag pipeline — a deploy-time
  decision, not a structural one).
- Full build: `mvn -T 1C verify` at the root. Scoped build: `mvn -pl <module> -am`.
- Container images via **jib-maven-plugin**: daemonless `jib:build` in CI,
  `jib:dockerBuild` for local Docker Desktop (§11).

### 3.2 Importing phase 1
`services/sequencer-nats` is the existing phase-1 sequencer imported into the
monorepo (preserve history via `git subtree add` or accept a squash import — team's
choice, document it). Two changes only, together = **milestone A0** (§15):
1. Its egress classes (`CoreNatsDestination`, `JetStreamDestination`,
   `BatchingDestination`, `DestinationChannel`, MessageBatch writer) move to
   `libs/nats-egress`, packages and tests preserved; the service depends on the
   sibling module.
2. Its build converts to a reactor module under `gcm-md-parent`.
No behavior change; its existing test suite is the proof. From A0 onward, phase-1 and
the nats-bridge compile against the *same source* of the egress code — the
depend-vs-vendor question is dissolved by construction.

### 3.3 Boundary enforcement (maven-enforcer + ArchUnit; CI-required)
- `libs/*` MUST NOT depend on `services/*` or on Spring.
- `sequencer-core` depends on Agrona only (§4).
- SBE-generated codecs (`md-models-sbe`) are **test scope** everywhere except
  `loadgen` (may use them at runtime for fixtures) — enforcer rule bans
  compile/runtime scope elsewhere; phase-1's "codecs never on the hot path" rule
  thereby becomes mechanical.
- `cluster-node` MUST NOT depend on Spring, jnats, or Kubernetes clients.

### 3.4 SBE codegen under Maven
`libs/md-models-sbe` runs `uk.co.real-logic:sbe-tool` via exec-maven-plugin at
`generate-sources` against `/schema/md-models-sbe-v4.xml` (the single canonical copy),
adds `target/generated-sources/sbe` via build-helper, ships as a normal jar.

## 4. `libs/sequencer-core` — the transport-agnostic stamping engine

The factored-out heart of the phase-1 sequencer and the body of the clustered
service. **Dependencies: Agrona only.** No Aeron, no NATS, no Spring, no clocks.
Everything it does is a pure function of (current state, input buffer, supplied time).

```java
public final class StampingEngine {
    /** All mutable state lives here; snapshotable, comparable. */
    public StampingEngine(StampingConfig cfg, EngineListener listener);

    /**
     * Process one ingress message in log order. May mutate buffer in place.
     * timeNanos MUST be cluster time (deterministic), never a local clock.
     * Verdict: STAMPED (publish), DUPLICATE (skip, no seq consumed),
     * REJECTED_SCHEMA (skip + count).
     */
    public Verdict onMessage(MutableDirectBuffer buf, int offset, int length,
                             long timeNanos, long sourceId);

    /** Emit a Heartbeat (templateId=4) into the supplied scratch buffer, stamped. */
    public int onHeartbeatTimer(MutableDirectBuffer scratch, long timeNanos);

    public void writeSnapshot(SnapshotSink sink);     // counter, eventId map, source map
    public void loadSnapshot(SnapshotSource source);
    public long currentSequenceId();
}
```

Contents (ported from phase-1 with the noted changes):
- **Sequence counter**: plain `long`, starts at snapshot value or configured origin.
  No block allocator — phase-1 §6 is deleted wholesale.
- **Stamping**: identical offset stores as phase-1 §4 (offsets from config: 8, 32;
  schemaId guard at 4; templateId at 2). `v3` profile support carries forward
  unchanged (`stamping.profile`, `timestamp-template-ids`).
- **Event enrichment**: phase-1 §8 logic verbatim (`Long2LongHashMap` eventId →
  firstSeq, cap + eviction metrics, stamp 56/64 on templateId 6). The map is now
  **replicated state**: include in snapshots with deterministic serialization order
  (sorted keys; tested, §12.3).
- **NEW — per-source ingress invariant** (promotion of phase-1's dedupe contract):
  maintain `Long2LongHashMap sourceId → lastSourceSeqNum` as replicated state.
  Config block `core.source-tracking`: enabled flag, per-template offsets for
  `sourceSeqNum` (default: enabled for templateId 9). Behavior:
  - `sourceSeqNum == last+1` → normal; advance.
  - `sourceSeqNum <= last` → **DUPLICATE**: do not stamp, do not consume a sequenceId,
    count `sequencer_source_duplicate_total{source}`. Makes line-handler
    republish-after-restart idempotent — this is what "simplifies the line handlers".
  - `sourceSeqNum > last+1` → stamp normally; count
    `sequencer_source_seq_gap_total{source}` (upstream-loss signal, phase-1 §13).
  - sourceId comes from the authenticated ingress identity (§7), never the payload.
- **Listener/metrics indirection**: the engine reports through `EngineListener`
  callbacks (single-writer, allocation-free); hosts map them to Micrometer. The
  engine never touches a metrics library.

Acceptance for this module alone: phase-1 offset-contract test, v3-profile test,
enrichment interleave test, plus the determinism tests (§12.3) — all with no
infrastructure.

## 5. `cluster-node` — the Aeron Cluster member

One deployable image running, in a single JVM: `ClusteredMediaDriver` (media driver +
Aeron Archive + consensus module, embedded) and the `ClusteredServiceContainer`
hosting `SequencerClusteredService`. Single-JVM embedded is the phase-2 baseline; a
standalone/C media driver is a documented colo-path upgrade behind a launcher
abstraction (§14), not built now.

### 5.1 `SequencerClusteredService`

```java
public final class SequencerClusteredService implements ClusteredService {
    // onStart: load snapshot into StampingEngine if provided; schedule heartbeat timer
    // onSessionMessage: resolve sourceId from ClientSession principal;
    //     engine.onMessage(buf, offset, length, clusterTimeNanos, sourceId);
    //     if STAMPED -> egressPublisher.publish(buf, offset, length)   (§6)
    // onTimerEvent (heartbeat): engine.onHeartbeatTimer(scratch, clusterTimeNanos)
    //     -> egressPublisher.publish(...); re-schedule via cluster.scheduleTimer
    // onTakeSnapshot: engine.writeSnapshot(...)
    // onRoleChange / onNewLeadershipTermEvent: egressPublisher.onRoleChange(...) (§6.4)
}
```

- **Cluster clock**: configure a **nanosecond-resolution `ClusterClock`** so
  `sequenceTimestamp` keeps its epoch-nanos contract at offset 32. The phase-1
  `OffsetEpochNanoClock` may back the clock at the consensus-module level (where time
  is agreed and logged); it is forbidden inside the service.
- **Heartbeat** (phase-1 §8) re-implemented on **cluster timers**
  (`cluster.scheduleTimer`); timer events are in the log, therefore deterministic.
  Heartbeats consume sequenceIds; `highWaterSequenceId` = engine.currentSequenceId().
- **Sessions**: clients map to sourceIds at session-open via a credentials/principal
  scheme (config `cluster.sources`: list of {name, sourceId, credential}). Reject
  unknown principals.

### 5.2 Cluster topology & Aeron configuration
- 3 members (5 supported by config; local profile allows 1 — a single-member cluster
  is valid Raft and keeps local dev light).
- **Static membership** via config; endpoints are **DNS names** from the Kubernetes
  headless service (§10), one per member
  (`gcm-md-seq-0.gcm-md-seq-hs.<ns>.svc.cluster.local:...`). Enable driver **name
  re-resolution** so member pod rescheduling (new IP, same name) reconnects without
  restart; covered by integration test (§12.2 follower-reschedule).
- All Aeron channels are **UDP unicast**; no multicast anywhere in this phase (AWS
  VPCs don't support it; egress fan-out uses MDC, §6.2). There is no TCP in Aeron.
- Driver settings externalized in chart values: term buffer lengths, MTU (align with
  pod MTU — 8k Aeron MTU under AWS 9001 jumbo frames; 1408 under default Docker
  Desktop MTU: the local values file must set this), `aeron.dir` on the mounted
  `/dev/shm` tmpfs, socket buffer sizes.
- **Archive** (embedded, per member) records the cluster log and the egress stream
  (§6.3). Archive + Raft log directories live on the member's PersistentVolume.

## 6. Egress — the sequenced stream

### 6.1 What egress is
A single Aeron publication carrying stamped messages **unbatched, in sequence order**
(`MessageBatch` no longer exists at this layer — batching moves entirely into the
bridge, §9). Stream identity in config: `egress.channel`, `egress.stream-id`.

### 6.2 Fan-out
MDC (multi-destination-cast) dynamic publication so N subscribers (bridge, fast
consumers, DR replicator) attach without config changes. At colo this flips to true
multicast by changing only `egress.channel` — nothing else may assume the transport.

### 6.3 Recording
The leader's Archive records the egress publication: (a) replay source for
late-joining consumers and the bridge after downtime, (b) input to prod-dr
replication (§13). Retention by size/time in values.

### 6.4 Replay & failover suppression — the critical invariant
Publishing is a side effect; log replay after restart and re-processing after
failover must never re-emit already-published messages. `EgressPublisher` is a
role-aware gate:
1. Publish only when `Cluster.role() == LEADER` **and** live (not replaying).
2. On assuming leadership (and on leader restart): query the local Archive for the
   egress recording's tail, read the **last recorded message's sequenceId** (offset 8
   via the engine's offset config), set `suppressUpTo = thatSeq` (absent/empty
   recording → snapshot floor).
3. While catching up with `engine.currentSequenceId() <= suppressUpTo`, drop
   publishes; resume at `suppressUpTo + 1`.
4. Metric `sequencer_egress_suppressed_total`; alarm if suppression occurs outside a
   role-change window.

Integration test (§12.2) kills the leader mid-stream and asserts **no sequenceId
appears twice and none is skipped** on recorded egress across failover — the heart of
phase-2 acceptance; must exist before any deploy job runs.

Back-pressure: `Publication.offer` retry with bounded spin-then-idle; persistent
blockage raises `sequencer_backpressure_stall_seconds_total` per phase-1 §9 `block`
mode. MDC with zero subscribers is not blockage (config
`egress.linger-on-no-subscribers`). Phase-1 `drop` mode is deleted — with contiguous
sequences it would violate §2.4.

## 7. `libs/cluster-client` — cluster ingress client

Thin wrapper over `AeronCluster` used by line handlers (migration phase C) and the
ingress-shim (phase A):
- `offer(DirectBuffer, offset, length)` with bounded retry/backoff and reconnect;
  surfaces cluster back-pressure to the caller (line handlers must be able to spill
  to a local buffer or slow their feed handler).
- Credentials → sourceId principal (§5.1).
- **Idempotent by construction**: because the service dedupes on `sourceSeqNum` (§4),
  the crash-recovery rule is "republish your tail since last known-processed".
  Document this as THE line-handler simplification, replacing phase-1's "redelivery
  gets a new sequenceId" caveat (now false; strike it from downstream docs).

## 8. `ingress-shim` (strangler step — temporary)

Spring Boot service, one replica, deleted at migration phase C: consumes `MD_RAW`
(JetStream, phase-1 named-durable semantics) and offers each message into the cluster
via `cluster-client` under a per-upstream-source sourceId (mapping in config). Ack to
JetStream only after the cluster offer is accepted; redelivery is harmless (dedupe).
Phase 2 therefore goes live **with zero line-handler changes** and enables the
parallel-run diff (§12.4).

## 9. `nats-bridge`

Spring Boot service; subscribes to the sequenced Aeron egress (live subscription;
Archive replay-merge to catch up after downtime, keyed by its last bridged sequenceId
checkpointed in a NATS KV key — the bridge is stateless-restartable, never
authoritative). Republishes to `MD_SEQUENCED`:
- **Depends on sibling module `libs/nats-egress`** — the same source the phase-1
  sequencer compiles against. `MessageBatch` (templateId=10) framing, flush policy,
  and `flush-on-event-boundary` are phase-1 §8 by construction; batching is now
  purely a bridge/JetStream concern.
- Bridge health asserts contiguity of consumed sequenceIds and alarms on any gap
  (`bridge_gap_total` — forever zero; nonzero means an egress bug).
- Existing JetStream/WebSocket consumers see no change whatsoever.

## 10. Kubernetes & Helm

One chart (`deploy/helm/gcm-md-sequencer-aeron`) with components toggleable per
values: `clusterNode` (StatefulSet), `ingressShim` (Deployment), `natsBridge`
(Deployment), RBAC, ServiceMonitors, PDB, NetworkPolicies. NATS itself is deployed
from `infra/nats-setup` (official NATS chart values + idempotent stream/KV setup
Job). Environment overlays in `environments/<env>/` — a commit of this repo fully
determines an environment.

Cluster-node StatefulSet requirements:
- `replicas` from values (local: 1 or 3; AWS envs: 3, one per AZ via
  `topologySpreadConstraints` on `topology.kubernetes.io/zone`).
- **Headless Service** (`clusterIP: None`) for per-pod DNS; member list templated
  from `replicas` + service name. **No ClusterIP Service may ever front Aeron
  traffic** — chart comment + NetworkPolicy permitting only direct pod-to-pod UDP on
  the Aeron port range.
- `/dev/shm`: `emptyDir: {medium: Memory, sizeLimit: <values>}`; per env (local
  512Mi; prod sized from term-buffer math — formula in `docs/runbooks/sizing.md`).
- `volumeClaimTemplates`: one PVC per member for Raft log + archive (local: Docker
  Desktop `hostpath` StorageClass; AWS: gp3, io2 option in prod values).
- **Guaranteed QoS** (requests == limits) in prod-class values; local values small
  (2 CPU / 4Gi) — must fit a laptop.
- Probes: liveness = process; readiness = member ACTIVE in a term and (leader only)
  egress connected; exposed via a tiny HTTP endpoint in cluster-node.
- `PodDisruptionBudget: maxUnavailable: 1`; `terminationGracePeriodSeconds` ≥
  snapshot + drain; preStop triggers clean `ClusteredServiceContainer` shutdown.
- JVM: ZGC generational, AlwaysPreTouch, per phase-1 §14.

## 11. Local deployment — Docker Desktop Kubernetes

Scripts in `deploy/local/`, orchestrated by the root Makefile. Constraint honored
throughout: **Docker Desktop's Kubernetes shares the Docker daemon's image store**,
so `jib:dockerBuild` images with `imagePullPolicy: IfNotPresent` need no registry.

Scripts (bash, `set -euo pipefail`, idempotent, re-runnable):
- `00-preflight.sh` — assert `kubectl config current-context == docker-desktop`
  (**hard fail otherwise — never touch a cloud context**); assert helm/docker/mvn
  present; warn if Docker Desktop has < 6 CPUs / 12 GiB; print the MTU note (§5.2).
- `10-build.sh` — `mvn -T 1C -DskipTests package jib:dockerBuild` at the root:
  builds everything and loads images (cluster-node, ingress-shim, nats-bridge,
  loadgen, nats-setup) tagged `local` into the local daemon.
- `20-install-nats.sh` — deploy `infra/nats-setup` into namespace
  `gcm-md-local`; run its stream/KV setup Job (idempotent).
- `30-deploy.sh` — `helm upgrade --install` the aeron chart with
  `environments/local/` values. Local profile: `clusterNode.replicas` 1 by default,
  `LOCAL_MEMBERS=3` env var switches (3-on-one-node supported for failover
  rehearsal); shim ON, bridge ON.
- `40-smoke-test.sh` — loadgen Job (e.g. 50k msgs across 2 simulated sources
  including deliberate duplicates); verifier Job asserts: (a) `MD_SEQUENCED`
  sequenceIds contiguous from origin, (b) duplicates absorbed (count matches),
  (c) heartbeat present with correct high-water mark. Non-zero exit on violation —
  the same verifier image runs in CI (§16).
- `50-failover-drill.sh` (3-member mode) — `kubectl delete pod` the leader
  mid-loadgen; re-run verifier; assert no duplicate/no gap (§6.4 as a laptop drill).
- `90-teardown.sh` — helm uninstall all releases, delete namespace, optional PVC wipe.

`make local-up` = 00→30; `make local-smoke` = 40; `make local-down` = 90.

## 12. Testing

### 12.1 Ported from phase-1 (must stay green)
Offset-contract test against generated v4 codecs; v3-profile test; enrichment
interleave test (all in `sequencer-core`); MessageBatch round-trip (in
`libs/nats-egress`, moved with the code); allocation profile: zero per-message
allocation attributable to `sequencer-core` and the cluster-node hot path.

### 12.2 Cluster integration suite (`integration-tests`; failsafe `*IT`; in-process Aeron cluster test harness, heavier cases also against the local 3-member profile)
- **Leader-kill contiguity** (upgraded phase-1 kill-9 test): kill -9 the leader under
  load; new leader continues with **no gap, no duplicate, no regression** on recorded
  egress. Phase-1's "gap ≤ block-size" tolerance is retired.
- **Egress no-double-publish** across failover and restart-replay (§6.4).
- **Follower reschedule / DNS re-resolution**: delete a follower, return with a new
  IP, assert rejoin and log catch-up.
- **Snapshot cycle**: force snapshot mid-load, restart member, state equals
  continuous-run state.
- **Ingress idempotency**: client republishes an overlapping tail; duplicates
  absorbed, zero sequenceIds consumed.

### 12.3 Determinism suite (new, load-bearing)
- **Replay equivalence**: same recorded log through two fresh service instances ⇒
  byte-identical stamped output and byte-identical snapshots.
- **Snapshot equivalence**: (snapshot at N, replay N+1..M) ≡ (replay 1..M) for
  counter, eventId map, source map.
- **Wall-clock/randomness ban**: ArchUnit rules — `sequencer-core` and
  `SequencerClusteredService` may not reference `System.nanoTime`,
  `System.currentTimeMillis`, `java.util.Random`, `java.time.Clock.system*`.

### 12.4 Parallel-run diff harness (migration gate)
Tool in `integration-tests/tools`: mirror one input stream into both
`services/sequencer-nats` and the shim→cluster path; join outputs on
(sourceId, sourceSeqNum); diff everything except sequenceId/sequenceTimestamp.
Because both paths compile against the same `libs/nats-egress` source, any diff
isolates ordering/stamping changes. Run in dev before phase-B cutover (§15); results
archived in `docs/migration/`.

### 12.5 Performance
JMH on the stamp path (unchanged target: low double-digit ns) and the cluster offer
path. Loadgen acceptance in uat: sustained 1M msgs/sec ingress→sequenced-egress with
p99 receive→egress latency reported; document multi-AZ vs single-AZ consensus cost
explicitly (expectation: multi-AZ quorum adds ~0.5–1 ms — record actuals).

## 13. Environments & DR

- **local/dev/uat/prod** per §10/§11; overlays in `environments/`.
- **prod-dr**: an independent standby cluster in the DR region. Raft quorum is never
  stretched cross-region. Feed: Aeron **Archive replication** of the egress recording
  (preferred) from prod; fallback: a bridge republisher. DR failover is
  **asynchronous and lossy at the tail**: messages sequenced but not yet replicated
  are lost; DR resumes from lastReplicatedSeq+1. This is the single permitted
  discontinuity in §2.1 — required runbook `docs/runbooks/dr-failover.md` states it,
  and the replicator exports `sequencer_dr_replication_lag_sequences`.

## 14. Portability guardrails (K8s → colo later)

- No AWS or Kubernetes API on any data path; K8s appears only in Helm and the
  readiness endpoint.
- All channels, endpoints, MTU, buffers, membership in values/config — never code.
- Nothing may assume multicast, or its absence (egress transport is one config key).
- Media-driver embedding sits behind a launcher abstraction so standalone/C driver is
  a launcher swap.

## 15. Migration plan (tracked as repo milestones)

- **A0 — Import & extraction (prerequisite)**: import phase-1 into
  `services/sequencer-nats` (§3.2); extract `libs/nats-egress`; convert to the
  reactor. No behavior change; phase-1 suite green.
- **A — Shadow**: deploy cluster + shim + bridge to dev publishing to
  `MD_SEQUENCED_SHADOW`; run the diff harness (§12.4) against phase-1 output.
- **B — Cutover**: bridge targets `MD_SEQUENCED`; phase-1 sequencer stops.
  Rollback = restart phase-1 sequencer (kept deployable until phase C; on rollback
  its KV high-water is seeded above the cluster's last issued seq — write this
  runbook).
- **C — Native line handlers**: line handlers adopt `cluster-client` (published from
  a tag pipeline if they live outside this repo), drop `MD_RAW` publishing; delete
  ingress-shim.
- **D — Fast consumers** subscribe to Aeron egress directly; NATS remains for
  replay/WebSocket only.

## 16. GitLab pipeline (single `.gitlab-ci.yml`)

Stages: `build → test → integration → package → deploy`. Jobs are **path-scoped**
with `rules: changes:` so a docs-only or env-values-only MR doesn't rebuild the world,
but `build`/`test` on the default branch always run the full reactor (the monorepo's
correctness guarantee). Cache the local Maven repo (`.m2/repository`), keyed on the
parent POM.

- `build`: `mvn -T 1C -DskipTests package` (full reactor on default branch;
  `-pl ... -am` scoped on MRs via changes rules).
- `test`: surefire unit + determinism suites (§12.1, §12.3). Enforcer/ArchUnit
  boundary rules (§3.3) run here. Required jobs — no allow_failure.
- `integration`: failsafe `*IT` (§12.2 in-process harness; Testcontainers NATS for
  shim/bridge). Runner needs DinD or a Docker socket; document both options in the
  file. Nightly schedule additionally runs the heavier failover drills. The §6.4
  no-double-publish and §12.3 replay-equivalence jobs are **required**; the pipeline
  must fail if they are skipped.
- `package`: `jib:build` (daemonless) pushing images tagged `$CI_COMMIT_SHORT_SHA` to
  `$CI_REGISTRY_IMAGE/<component>`; `helm lint` + `helm package` with
  `appVersion=$CI_COMMIT_SHORT_SHA`.
- `deploy` (GitLab K8s agent contexts per environment; every deploy uses images and
  values from the *same commit* — the SHA is the BOM):
  - `deploy:dev` — auto on default branch: nats-setup apply, then the chart with
    `environments/dev/`; post-deploy runs the containerized smoke verifier (§11's
    40-script).
  - `deploy:uat` — manual; additionally runs loadgen acceptance and publishes the
    latency report artifact; required before prod.
  - `deploy:prod` — manual, protected environment, requires uat green on the same SHA.
  - `deploy:prod-dr` — manual; standby profile + replicator.
- Tag pipeline (`v*`): additionally publishes `cluster-client` + `md-models-sbe` jars
  to the GitLab Maven registry for external consumers (migration phase C).

## 17. Observability

Carry phase-1 §13 metrics forward where meaningful; add: `sequencer_cluster_role`
(gauge per member), `sequencer_commit_position`, `sequencer_snapshot_duration_seconds`,
`sequencer_egress_suppressed_total`, `sequencer_source_duplicate_total{source}`,
`sequencer_source_seq_gap_total{source}`, `bridge_gap_total`, `bridge_lag_sequences`,
`sequencer_dr_replication_lag_sequences`. Export Aeron's own counters (errors,
back-pressure, flow control) via the counters reader → Prometheus in cluster-node.
Grafana dashboard JSON in `deploy/observability/`.

## 18. Acceptance criteria (phase-2 definition of done)

1. From a fresh `git clone`: `make local-up && make local-smoke` passes on clean
   Docker Desktop, in 1-member and 3-member modes; `50-failover-drill.sh` passes in
   3-member mode.
2. All §12.1 phase-1 tests green; §12.3 determinism suite green; enforcer/ArchUnit
   boundary, clock/random, and codec-scope rules enforced in CI.
3. Leader-kill contiguity and egress no-double-publish tests green as required CI
   jobs, and demonstrated in dev by the post-deploy verifier.
4. Ingress idempotency demonstrated: overlapping republish absorbs duplicates, zero
   sequenceIds consumed.
5. Milestone A0 complete: phase-1 imported, `libs/nats-egress` extracted, both
   consumers compile against it, phase-1 suite green.
6. Bridge output verified byte-compatible with phase-1 `MD_SEQUENCED` consumers via
   the §12.4 diff harness in dev shadow mode.
7. uat loadgen report: sustained 1M msgs/sec with documented latency percentiles and
   documented multi-AZ consensus cost.
8. Pipeline deploys dev automatically and uat/prod/prod-dr via manual gates; the
   commit SHA is the BOM end-to-end (images, chart, values); rollback runbook (§15-B)
   written and rehearsed in dev.
9. Runbooks delivered: sizing, member replacement, snapshot management, DR failover,
   rollback-to-phase-1.

## 19. Deferred / watchlist

- **Aeron Sequencer (Adaptive product)**: announced Jan 2026, pre-GA. Our
  `sequencer-core` + deterministic `ClusteredService` shape is deliberately aligned
  with Adaptive's stated Cluster↔Sequencer portability; re-evaluate at GA. Track
  their promised open-source portability abstraction.
- C media driver / kernel bypass (colo), true multicast egress (colo), `reserved1`
  partition/epoch usage (stays frozen), per-symbol fan-out tier, replay/gap-request
  service.
