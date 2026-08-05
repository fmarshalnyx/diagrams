# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

`gcm-md-parent` reactor: a market-data **sequencer** — assigns a globally monotonic
`sequenceId` to every SBE-encoded market-data message it receives, stamps it directly into
the binary message at fixed byte offsets (never decoding the body), and republishes the
stamped stream as the canonical, totally-ordered record for downstream consumers. Target:
1,000,000 msgs/sec sustained, ~1µs/message hot-path budget.

The full design rationale lives in `docs/SEQUENCER-PROJECT.md` (phase 1, NATS-based —
implemented as `services/sequencer-nats`), `docs/AERON-SEQUENCER-DESIGN.md` (phase 2,
Aeron Cluster/Raft — implemented as `services/sequencer-aeron/{cluster-node,nats-bridge}`,
deployed via `deploy/helm/gcm-md-sequencer-aeron`; see "Phase 2: Kubernetes deployment" below
for what's actually been run against a live cluster vs. written-but-unvalidated), and
`docs/AERON-SEQUENCER-IMPLEMENTATION-STEPS.md` (the phase-2 milestone plan — **its "Current
status & path to completion" section, near the top, is the single source of truth for what's
done and what's left across the whole application; check there before assuming any milestone's
state from memory**. See its Milestone 5 for why there's no `ingress-shim` service: line
handlers pick their ingress transport,
Aeron cluster or NATS, by config via `libs/ingress-transport`, instead of migrating through a
temporary strangler service — right-sized for this project's single-team, pre-launch, no-
external-clients state, not a general recommendation; also see Milestone 5B for the
`line-handler-template` + `mock-upstream-source` pair that gives real line-handler authors a
runnable reference to copy, and unblocks load/flow validation for both phase-1 and phase-2
without waiting on that external work). `services/sequencer-aeron/bench` is phase-1 support
tooling (JMH). `services/sequencer-aeron/loadgen` was retired in Milestone 5B.4 —
`mock-upstream-source` absorbed both of its jobs (traffic generation and contiguity/dedup
verification on observed egress) for phase-1 and phase-2 alike, and now backs
`50-failover-drill.sh` too. Read the relevant spec before changing hot-path, schema-offset, or
failover behavior — these documents are treated as authoritative and the code is expected to
match them exactly (e.g. offsets must come from config, never be hardcoded).

## Build

- `mvn clean install` — build everything
- `mvn -pl services/sequencer-nats -am install` — build the sequencer service and its deps
- `mvn test` — run unit tests (repo-wide, excludes `*IT.java`)
- `mvn -pl services/sequencer-nats test` — unit tests for one module
- `mvn -pl services/sequencer-nats test -Dtest=SequenceAllocatorTest` — single test class
- `mvn -pl services/sequencer-nats spring-boot:run` — run the sequencer locally (needs a
  reachable NATS + Kubernetes context; `-Dspring-boot.run.profiles=<env>` selects
  local/dev/uat/prod/prod-dr)
- Integration tests (`*IT.java`) run only under `maven-failsafe-plugin` (`verify`), never
  under `test`/surefire — this exclusion is enforced in `build/gcm-md-parent/pom.xml`.
- `sequencer-bench` (JMH) is a standalone validation tool, not part of the service build/deploy
  path — see its README under `services/sequencer-aeron/`.

## Module layout (Maven reactor)

- `build/gcm-md-parent` — parent POM: Java 25 compiler config, all dependency versions
  (Spring Boot, Agrona, Aeron, jnats, fabric8, SBE tool, JMH, Testcontainers, ArchUnit),
  and shared plugin config (surefire excludes `*IT`, failsafe runs them, enforcer requires
  dependency convergence).
- `libs/md-models-sbe` — canonical SBE schema module (`schema/md-models-sbe-v4.xml` is the
  repo-root copy of the schema this and everything else builds against; schemaId=100, v4,
  little-endian). Generated codecs are used only in tests/benchmarks — never on the hot path.
- `libs/nats-egress` — Spring-free egress primitives (`DestinationChannel`,
  `CoreNatsDestination`, `JetStreamDestination`, `BatchingDestination`) consumed by
  `services/sequencer-nats` via lean config records (`EgressConfig`, `BatchingConfig`), so
  the hot-path egress code carries no Spring dependency. Also hosts `NatsIngressTransport` +
  `NatsIngressConfig` — the NATS-side `libs/ingress-transport` implementation a line handler
  can select by config as an alternative to `libs/cluster-client`'s `ClusterIngressClient`;
  reuses this module's `EgressMetrics` listener pattern and the same bounded in-flight
  `JetStream.publishAsync` window as `JetStreamDestination`, but surfaces a full window to the
  caller as backpressure (never blocks or drops) to match `ClusterIngressClient`'s contract.
- `libs/ingress-transport` — the `IngressTransport` interface
  (`offer(DirectBuffer, offset, length)`, non-negative on success, negative on backpressure)
  line handlers code against, independent of which concrete transport is wired in at startup.
  Deliberately tiny and dependency-free (Agrona only) so `libs/cluster-client` and
  `libs/nats-egress` can each implement it without depending on each other. See its
  `package-info.java` for why this replaced the originally-planned `ingress-shim` service.
- `libs/sequencer-core` — the phase-2 transport-agnostic stamping engine (package
  `gcm.md.sequencer.stamping`; design doc §4). `StampingEngine` is a pure function of
  (current state, input buffer, supplied time nanos) — no clocks, no I/O, Agrona-only
  dependency — so it's identical logic to phase-1's `SequenceStamper` plus a new per-source
  dedupe/gap invariant (`onMessage` returns `STAMPED`/`DUPLICATE`/`REJECTED_SCHEMA`), and is
  deterministically snapshotable (`writeSnapshot`/`loadSnapshot`, sorted-key serialization),
  now embedded by `services/sequencer-aeron/cluster-node`'s `SequencerClusteredService`.
  `services/sequencer-nats` is untouched and still runs its own phase-1 `SequenceStamper`.
- `libs/cluster-client` — thin wrapper over `AeronCluster` for line handlers, package
  `gcm.md.sequencer.clusterclient` (design §7). `ClusterIngressClient` implements
  `libs/ingress-transport`'s `IngressTransport` — the Aeron-side option a line handler's
  config can select, alongside `libs/nats-egress`'s `NatsIngressTransport`. `offer` retries
  backpressure only a *bounded* number of times before surfacing it to the caller (unlike
  the egress side's unbounded block — a line handler's feed thread must stay free to spill
  or slow down) and reconnects on `NOT_CONNECTED`/`CLOSED`. `SourcePrincipalCredentialsSupplier`
  supplies the credential `cluster-node`'s `SourcePrincipalRegistry` maps back to a sourceId;
  the two are configured to agree but share no code (`libs/*` can't depend on `services/*`).
  The idempotency contract ("republish your tail since last known-processed" — safe because
  the engine dedupes on `sourceSeqNum`) is documented in this package's `package-info.java`;
  it supersedes phase-1's "redelivery gets a new sequenceId" caveat once a line handler's
  config switches it onto this transport. A production `ClusterIngressClient` also needs an
  embedded Aeron `MediaDriver` in its own process (`AeronCluster.connect()` doesn't launch one
  implicitly) — see `services/sequencer-aeron/integration-tests`'s `TestIngressClients` for the
  pattern, since there's no longer a shim service to copy it from.
- `libs/architecture-tests` — ArchUnit boundary rules complementing the maven-enforcer bans
  above at the source-import level. Currently `@Disabled`: ArchUnit (through the latest
  published 1.4.2) can't parse this reactor's Java 25 class files, so the maven-enforcer
  rules are the sole *working* enforcement of `libs/*` boundaries until that's fixed
  upstream — see the class Javadoc on `ModuleBoundaryTest`.
- `services/sequencer-nats` — the actual sequencer service (Spring Boot). Package root
  `gcm.md.sequencer`: `config/` (`SequencerProperties` + `ServiceConfiguration`, the sole
  bean-definition source), `core/` (`SequenceStamper`, `SequenceAllocator`,
  `SequencerPipeline`, `LeaderElection`), `ingress/`, `egress/` interfaces wired to
  `libs/nats-egress`, `heartbeat/`, `metrics/`, `health/`. Five environment profiles
  (`local`, `dev`, `uat`, `prod`, `prod-dr`) selected via `SPRING_PROFILES_ACTIVE`, each with
  its own `application-{env}.yml` and `k8s/overlays/<env>/`.
- `services/sequencer-aeron/bench` — phase-1 support tooling only (JMH benchmark of
  `SequenceStamper.stamp()`), unrelated to the `cluster-node` module below despite the
  shared parent directory.
- `services/sequencer-aeron/cluster-node` — the phase-2 Aeron Cluster member (design §5;
  package `gcm.md.sequencer.cluster`). `ClusterNodeLauncher` embeds a single-JVM
  `ClusteredMediaDriver` + `ClusteredServiceContainer` hosting `SequencerClusteredService`,
  which wraps `libs/sequencer-core`'s `StampingEngine` with cluster session/timer/snapshot
  plumbing. No Spring, no jnats, no Kubernetes client (enforced by a module-specific
  maven-enforcer rule in this module's own `pom.xml` — Raft replaces both the NATS KV
  fencing checkpoint and the Kubernetes Lease phase-1 used). `NoOpEgressPublisher` remains
  available for tests/stub profiles; `ClusterNodeLauncher.main` wires the real
  `AeronEgressPublisher` (Milestone 3, design §6): a single MDC dynamic publication,
  recorded by the local Archive, gated by `SuppressionGate` (design §6.4's "no sequenced
  message is ever published twice" invariant — pure decision logic, fully unit-tested) and
  `ArchiveRecordingTailQuery` (finds the last-published sequenceId on leadership assumption
  via a fixed well-known Archive session id, `AeronEgressPublisher.EGRESS_SESSION_ID`, so a
  new leader can compute where to resume). **Confidence note:** `ArchiveRecordingTailQuery`
  and the backpressure-retry loop in `AeronEgressPublisher` are written against the verified
  Aeron Archive client API but unverified against a live Archive/cluster — no cluster has
  been started in this environment. The design's own §12.2 leader-kill integration test
  (kill the leader mid-stream, assert no sequenceId is ever duplicated or skipped on
  recorded egress) is the required gate on this class before trusting it in a real
  failover — see the confidence note in `ArchiveRecordingTailQuery`'s Javadoc. Tests
  otherwise exercise `SequencerClusteredService`/`SuppressionGate`/`ArchiveRecordingTailQuery`
  logic against Mockito fakes (no live Aeron networking); the real embedded/multi-member
  cluster runtime tests belong in the dedicated `services/sequencer-aeron/integration-tests`
  module (design §12.2), not here.
- `services/sequencer-aeron/nats-bridge` — Spring Boot service (design §9, Milestone 6)
  subscribing to the cluster's sequenced Aeron egress and republishing to `MD_SEQUENCED`, so
  existing JetStream/WebSocket consumers see no change. Package root `gcm.md.natsbridge`.
  `BridgePipeline` uses Aeron's built-in `ReplayMerge` to catch up from the leader's Archive
  recording after downtime, then continues on the live `Image` once merged — the standard
  Aeron tool for this, not hand-rolled replay bookkeeping. It always replays its target
  recording from position 0 rather than translating its NATS-KV-checkpointed sequenceId into
  an exact Archive byte position (no clean way to do that translation here);
  `ContiguityTracker` cheaply skips everything at/before the checkpoint instead, at the cost
  of bounded (recordings are retention-bounded) replay work on every restart. Depends on
  `libs/nats-egress` for `MessageBatch` framing — same source phase-1 and cluster-node's
  `AeronEgressPublisher` both compile against. `BridgeMetrics.onGapDetected` /
  `bridge_gap_total` should be permanently zero (design §9: phase-2 egress is contiguous by
  construction) — nonzero means an egress bug, not bridge noise. **Confidence note:** like
  `cluster-node`'s Archive-touching classes, the `ReplayMerge`-driving loop in
  `BridgePipeline.run` is unverified against a live Archive/cluster; the skip/bridge/gap/
  checkpoint decision itself (`onFragment`) is deliberately factored out so it's fully
  unit-tested without live Aeron — see the class Javadoc.
- `services/sequencer-aeron/line-handler-template` — Spring Boot reference service
  (implementation-steps.md Milestone 5B), package `gcm.md.linehandlertemplate`. The first
  concrete `@ConditionalOnProperty`-selected `IngressTransport` bean pair in this reactor
  (`line-handler.ingress-transport: aeron|nats`); `UpstreamRelay` fetches from a durable
  JetStream pull consumer and only acks after a successful `offer()`, so crash-recovery needs
  zero local bookkeeping (JetStream redelivery + the sequencer's `sourceSeqNum` dedup do the
  whole job). Deployed independently of `mock-upstream-source` — separate Helm Deployment, own
  toggle/image/replicas. See the module's README for what a real line handler should copy.
- `services/sequencer-aeron/mock-upstream-source` — persistent Spring Boot service
  (implementation-steps.md Milestone 5B), package `gcm.md.mockupstreamsource`. Continuously
  generates synthetic upstream traffic (steady/bursty, with deliberate gap/duplicate injection)
  and continuously verifies contiguity/no-duplicates on the sequencer's final observed egress,
  reported as `mock_upstream_gap_total`/`mock_upstream_duplicate_total` gauges (should be
  permanently zero — same "nonzero means a real bug" contract as `bridge_gap_total`; the
  duplicate gauge in particular currently reads a small nonzero rate under sustained load, a
  known, tracked artifact of `EgressConsumer`'s JetStream at-least-once redelivery rather than a
  system-level double-publish — see the implementation-steps doc). Fully replaced
  `services/sequencer-aeron/loadgen` (retired in Milestone 5B.4) — a genuine continuous service
  (no `--duration-seconds`, no exit code — scale the Deployment to `0` to turn it off), deployed
  independently of `line-handler-template`.

## Architecture — hot path discipline

This is the load-bearing design constraint across the whole codebase; violating it in a
change is the most likely way to break the project's actual purpose:

- **Single-threaded stamping, zero handoffs.** The NATS client's delivery thread receives,
  stamps, hands to the batcher, and publishes async — no queues, no context switches, no
  Disruptor/ring buffer unless benchmarks prove one is needed.
- **Never decode SBE on the hot path.** `SequenceStamper` wraps the raw `byte[]` in an
  Agrona `UnsafeBuffer` and writes `sequenceId`/`sequenceTimestamp` at fixed offsets via
  `putLong(offset, value, LITTLE_ENDIAN)`. Generated SBE codecs exist only in tests/JMH
  setup, to *prove* configured offsets match the schema — never in `main` hot-path code.
- **All offsets and template rules come from config** (`sequencer.stamping.*` in
  `application.yml`), compiled to plain `int`/`boolean` fields once at startup. Do not
  hardcode offsets (8, 32, 56, 64) or template IDs (e.g. boundary templateId 6) in code.
- **Timestamps via Agrona `OffsetEpochNanoClock`** — never `System.nanoTime()` (not
  epoch-based) or bare `currentTimeMillis()`.
- **Allocation-free steady state** for all sequencer-owned code (reused buffers, primitive
  counters, no boxing/lambda capture on the hot path). The NATS client's own per-message
  `byte[]` allocation is a documented, accepted exception — don't try to eliminate it.
- **Gaps allowed, duplicates/regression never.** Sequence IDs are assigned via high-water-mark
  block leasing (NATS KV, compare-and-swap), not per-message persistence — see
  `SequenceAllocator` and spec §6.
- **Single writer, fenced.** Kubernetes Lease-based leader election (`LeaderElection`) gates
  every KV block lease; on lease loss, ingress drain-stops *before* publishing stops, never
  the reverse (`SequencerPipeline` owns this ordering).

## Phase 2: Kubernetes deployment (design §10, §11 — Milestones 7/8)

`deploy/helm/gcm-md-sequencer-aeron` (clusterNode StatefulSet, natsBridge Deployment, headless
Service, NetworkPolicy, PDB — each toggleable via values) + `infra/nats-setup` (official NATS
chart + an idempotent Job creating `MD_RAW`/`MD_SEQUENCED` streams and the
`sequencer-lease`/`bridge-checkpoint` KV buckets) + `deploy/local/*.sh` + root `Makefile`
(`make local-up` / `local-smoke` / `local-down`), per design §11. (No `ingressShim` Deployment
— see the "What this is" section above: line handlers pick their transport by config and
deploy independently of this chart.)

**Real bugs found under live testing (Docker Desktop Kubernetes), not code review, each fixed
in source** — accumulated across sessions, kept here since they're non-obvious and easy to
reintroduce:

- Bare `jib:dockerBuild` invoked as a CLI goal runs against *every* reactor module, not just
  the modules that declare it (`cluster-node`, `nats-bridge`, `line-handler-template`,
  `mock-upstream-source`) — fixed by binding it to the `package` phase in those modules only.
- Docker Desktop's Kubernetes node caches the `local` tag's digest mapping; `IfNotPresent`
  never re-resolves it after a rebuild, so a redeployed pod silently keeps running the old
  image. Fixed by using `imagePullPolicy: Always` for local (still resolves against the local
  daemon, no registry needed) — see `environments/local/values.yaml`'s comment.
- `nats kv add` (unlike `nats stream add`) has no `--defaults` flag — `infra/nats-setup`'s
  setup script was passing it and failing.
- `Archive.Context.replicationChannel` must be set independently of
  `ConsensusModule.Context.replicationChannel()` — same field name, two unrelated Aeron
  requirements; `cluster-node` crashed hard on startup without it.
- The consensus module's *own* in-process connection to the co-located Archive must use
  `aeron:ipc`, not UDP ("local archive control must be IPC") — the external-facing
  `archiveControlChannel` (UDP, for remote clients like `nats-bridge`) is a separate concern.
- `ConsensusModule.Context.ingressChannel()`/`logChannel()` must be endpoint-less *templates*
  (e.g. `aeron:udp?term-length=64k`) — the actual per-member bind endpoints come from each
  member's own field in `clusterMembers`. Setting an explicit endpoint on them silently left
  the ingress port unbound (confirmed via `/proc/net/udp` and `ClusterTool describe/errors`
  inside the pod — genuinely useful for debugging Aeron cluster startup issues live).
- **Aeron forbids `scheduleTimer`/sending messages from every lifecycle callback** (`onStart`,
  `onRoleChange`, `doBackgroundWork`) — confirmed via `ClusterTool errors`, not documented
  anywhere obvious. Only `onSessionMessage`/`onTimerEvent` permit it. `SequencerClusteredService`
  now bootstraps the first heartbeat timer opportunistically from the first ingress message
  seen as leader; a fully idle cluster with zero ingress traffic ever emits no heartbeat at
  all — a known, accepted gap (see the class Javadoc on `ensureHeartbeatScheduled`).
- `nats-bridge` never launched an embedded Aeron media driver — `Aeron.connect()` expects one
  already running, it doesn't launch one implicitly. Any `libs/cluster-client` consumer
  (a line handler on the Aeron transport) needs the same — see that module's note above.
- `nats-bridge`'s live-egress subscribe channel used `endpoint=` (bind semantics) instead of
  `control=...|control-mode=dynamic` (matching cluster-node's actual dynamic-MDC publisher
  channel) — `endpoint=<cluster-node's-address>` tried to bind a socket on an address the
  bridge's own pod doesn't own, which fails immediately ("Address not available").
- Health probes had no `initialDelaySeconds`; Spring context + Aeron client/embedded-driver
  init takes ~45s locally, so the liveness probe killed the service before startup ever
  finished — an infinite crashloop that looks like an app bug but is a probe-timing bug.
- `helm uninstall` has no `--ignore-not-found` flag (unlike `kubectl delete`) — `90-teardown.sh`
  had it; harmless under `|| true` but the actual error masked was "unknown flag", not "release
  not found" as intended.

**What's confirmed genuinely working**: both single-member (`ClusterNodeConfig.singleMember`)
and multi-member (`ClusterNodeConfig.kubernetesMember`, `CLUSTER_MEMBERS` env var parsed by
`ClusterNodeLauncher`) deployments — a 3-member cluster starts in real pods, embeds its media
driver + Archive + consensus module per member, elects exactly one leader via real Raft, and
binds all Aeron ports on each pod's real DNS address — verified directly via `ClusterTool
is-leader`/`list-members` inside the running pods, including an observed leadership handover
across a rolling restart, not just "pods didn't crash." Full account, citations, and what's
still deferred: `docs/AERON-SEQUENCER-3-MEMBER-CLUSTER-STATUS.md`.

**What's not yet validated**: an ingress-to-`MD_SEQUENCED` run driven by something other than
the in-process `*IT` suite — needs a real line handler or Milestone 5B's
`line-handler-template` + `mock-upstream-source` pair (neither built yet) actually deployed and
exercising the cluster's ingress path in Kubernetes, which nothing currently does now that
`ingress-shim` is gone; the `ReplayMerge`/replay-from-Archive path in `BridgePipeline` (only the
"no recording yet, live-only" fallback path has been exercised); `50-failover-drill.sh`.
Picking this back up should start with `make local-up` from a clean namespace.

## Conventions

- Services are Maven submodules; package convention `src/main/java/gcm/md/<service>/`.
- REST controllers (if any) go in `.../controller/`.
- All public methods need Javadoc.
- **All beans are defined in one `ServiceConfiguration` class** per service
  (`src/main/java/gcm/md/<service>/config/ServiceConfiguration.java`) — no
  `@Autowired`, no stereotype-annotated (`@Component`/`@Service`) business classes;
  every collaborator is wired explicitly via constructor injection through `@Bean` factory
  methods. See `services/sequencer-nats/.../config/ServiceConfiguration.java` for the
  pattern to follow.
- No hardcoded values — use `application.yml` (`SequencerProperties`-style
  `@ConfigurationProperties`), overridable per-environment via
  `application-{profile}.yml`, env vars, or CLI args (Spring's standard precedence order).
- Module boundary rule (from the phase-2 design, applies going forward): `libs/` must never
  depend on `services/`; hot-path modules must not depend on Spring or SBE codecs at runtime.
