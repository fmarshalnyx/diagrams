# md-sequencer

GCM-MD sequencer service: assigns a globally monotonic, sequential `sequenceId` to every
SBE-encoded market-data message it receives, stamps it directly into the binary message, and
republishes the stamped stream as the canonical, totally-ordered record for all downstream
consumers. See [`SEQUENCER-PROJECT.md`](../SEQUENCER-PROJECT.md) at the repo root for the full
design spec this module implements; this README is the operational reference.

## What it does

1. **Receives** SBE-encoded market-data messages (schema `md-models-sbe`, v4) from an ingress
   channel — NATS core subscription or a JetStream durable pull consumer.
2. **Assigns** a sequenceId to every message that is monotonic and sequential across *all*
   messages, *all* instruments, and *all* sources — one totally ordered stream. Gaps are allowed
   (failover, restarts); regression and duplicates are never allowed.
3. **Stamps** the sequenceId (and a sequencing timestamp) directly into the binary message at
   fixed byte offsets — it never decodes the message body to do this.
4. **Publishes** the stamped message to a pluggable destination — core NATS or JetStream, with an
   optional batching layer — so downstream consumers see one canonical, gap-detectable stream.
5. **Emits its own heartbeat** on the sequenced stream so a quiet market ("no data") is
   distinguishable from an outage ("missing data").

It is deployed as two Kubernetes replicas (one active, one warm standby) coordinated by a
Kubernetes Lease, so there is always exactly one writer.

## How it does it

### Hot path: stamping without decoding

`core/SequenceStamper` is the entire per-message logic. It wraps the raw byte array delivered by
the NATS client in an Agrona `UnsafeBuffer` and writes two fields at fixed absolute offsets —
`sequenceId` and `sequenceTimestamp` — using `putLong(offset, value, LITTLE_ENDIAN)`. It never
instantiates a generated SBE codec; those exist only in the test suite, used to *prove* the
configured offsets line up with the schema. All offsets, the schemaId sanity guard, and the
MatchEventBoundary enrichment rule are compiled out of config once at startup into plain `int`
fields, so there is no config lookup, branch on template metadata, or allocation per message.

A schemaId guard runs on the first message after every (re)subscribe (optionally every message,
see `validate-schema-id-per-message` below) — a mismatch means the wrong upstream stream got
wired to this ingress, and the message is dropped rather than corrupting foreign bytes; if it's
the first message ever seen, the service fails readiness instead of silently running degraded.

### Sequence assignment: block leasing, not per-message persistence

`core/SequenceAllocator` hands out sequenceIds from an in-memory `long` counter. Since gaps are
tolerated, the service doesn't persist every assignment — instead it leases a *block* of IDs
(`allocator.block-size`, default 1,000,000) from a NATS JetStream KV bucket via compare-and-swap,
writes "leased up to N" once per block (~1 KV write/sec at full 1M msg/sec throughput), and hands
out IDs from that block locally. The next block is leased **proactively** in the background once
`allocator.lease-ahead-fraction` of the current block is consumed, so block rollover never stalls
the hot path on KV round-trip time. A crash costs at most one block's worth of gap; sequenceIds
stay strictly monotonic across restarts.

Every lease attempt is fenced by leadership: `core/LeaderElection` (a Kubernetes Lease via the
fabric8 client) is checked before every KV write, so a wedged old leader can't corrupt the
high-water mark after losing the lease.

### Egress: pluggable destination, optional batching

`egress/DestinationChannel` is implemented by `CoreNatsDestination` (fire-and-forget, loss is
possible but immediately detectable via sequenceId gaps) and `JetStreamDestination` (async publish
with a bounded in-flight ack window). `egress/BatchingDestination` optionally decorates either one,
packing N stamped messages into one `MessageBatch` (templateId=10) envelope using hand-rolled
`UnsafeBuffer` writes against one reused direct buffer — this is what makes ~1M logical msgs/sec
achievable on JetStream, whose cost is per-publish rather than per-byte. A batch flushes on
whichever comes first: `max-messages`, `max-bytes`, `max-linger-micros`, or (if enabled)
immediately after a `MatchEventBoundary` so one matching-engine event never straddles two batches.

Backpressure (`egress.backpressure`) is either `block` (stall the hot thread until the destination
has room — preferred, since consumers would rather see a brief stall than silent unreplayable
loss) or `drop` (never stall; the sequenceId is still consumed, so downstream sees the drop as a
normal gap).

### Ingress and the redelivery/dedupe contract

`ingress/NatsSequencerIngress` supports two modes: `jetstream` (a named-durable pull consumer,
which doubles as a fencing layer — JetStream rejects a second active puller on the same durable)
and `core` (plain at-most-once subscription for lower-latency inputs with no ack machinery). In
JetStream mode, a fetched batch is acked only *after* every message in it has cleared egress —
for JetStream egress, only after publish acks are back. If the process crashes between receiving a
message and publishing its stamped form, that message is **redelivered** on restart/failover and
gets a **new** sequenceId, since the sequencer has no way to know it already saw that payload (it
never decodes bodies).

**This means a duplicate payload can appear under two different sequenceIds.** Downstream
consumers that need exactly-once semantics must dedupe using the schema-carried key
`(source-scope) instrumentId + sourceSeqNum` — never assume sequenceId alone implies uniqueness of
*payload*, only uniqueness and total order of *delivery*.

### Startup ordering and single-writer enforcement

`core/SequencerPipeline` runs this sequence on a background thread (so it never blocks Spring
context refresh or the actuator port from coming up), and readiness is reported only once all four
steps complete:

1. Verify the ingress and egress NATS connections are up, and ensure the allocator's KV bucket
   exists.
2. Acquire the Kubernetes leadership lease (blocks here on a standby replica until failover).
3. Lease the first sequence block from KV.
4. Subscribe ingress.

On losing leadership, ingress is drain-stopped **first**, then publishing stops, then readiness
drops — never the reverse, so the service never consumes what it can no longer publish.

The stamp-allocate-publish critical section is guarded by a lock. This is a deliberate, documented
deviation from a bare-`long`-no-atomics single-writer design: it exists solely so the
independently-timed heartbeat (see below) can interleave safely with the ingress thread. The lock
is uncontended in the overwhelming common case (heartbeats fire every 100ms by default, not
per-message), so its cost is negligible against the microsecond-per-message budget.

### Heartbeat

`heartbeat/HeartbeatEmitter` builds a `Heartbeat` (templateId=4, `heartbeatType=SEQUENCER`) message
carrying the current high-water sequenceId, on a timer independent of ingress traffic. It is fed
through the *exact same* stamp-and-publish path as any ingress message — it consumes a sequenceId
like any other message — which is also how a fully idle stream still gets its buffered batch
flushed (see `BatchingDestination`'s linger flag).

### Observability

Micrometer + Prometheus, exposed at `/actuator/prometheus`: message/byte/batch counters, current
sequenceId, in-flight JetStream window, backpressure stall time, dropped/failed-publish counts,
lease successes/failures, schemaId mismatches, per-source `sourceSeqNum` gap counts, and
receive→publish latency percentiles (p50/p99/p99.9/max) tracked via an off-thread-drained
HdrHistogram so the hot path only ever does a wait-free `recordValue` call. Readiness/liveness are
separate actuator health groups; a custom health indicator additionally reports ingress/egress
connectivity, leadership, and whether the first message ever received passed the schemaId guard.

## Deployment environments

The service runs, unmodified, in five Kubernetes environments: **local** (Kubernetes in Docker
Desktop), **dev**, **uat**, **prod**, and **prod-dr** (all AWS/EKS). Which one is active is
selected entirely by the `SPRING_PROFILES_ACTIVE` environment variable (set per-environment by
the matching `k8s/overlays/<env>/kustomization.yaml`), which activates the corresponding
`application-{env}.yml` bundled in the jar — that file is the single place each environment's NATS
endpoints, Kubernetes leadership namespace, and `environment` metrics/info tag are defined:

| Environment | Profile | Namespace | NATS | Notes |
|---|---|---|---|---|
| Local (Docker Desktop) | `local` | `md-sequencer-local` | in-cluster `nats:4222`, deployed alongside the service | 1 replica; no HA/DR concerns |
| Dev | `dev` | `md-sequencer-dev` | `nats.dev.md-platform.internal:4222` (placeholder) | 2 replicas, 1 CPU / 2Gi |
| UAT | `uat` | `md-sequencer-uat` | `nats.uat.md-platform.internal:4222` (placeholder) | 2 replicas, 2 CPU / 4Gi |
| Prod | `prod` | `md-sequencer` | `nats.prod.md-platform.internal:4222` (placeholder) | 2 replicas, 4 CPU / 8Gi, static-CPU-manager node pool |
| Prod DR | `prod-dr` | `md-sequencer-dr` | `nats.prod-dr.md-platform.internal:4222` (placeholder, **independent cluster from prod**) | Same tier as prod; own leadership Lease, own NATS, no cross-region coordination with prod |

See [`k8s/README.md`](k8s/README.md) for the full manifest structure (Kustomize base + one
overlay per environment), what to fill in before deploying (ECR account IDs, real NATS DNS
names), and why `prod-dr` is a fully independent deployment rather than "prod with more replicas."

To run any profile locally without Kubernetes at all:

```
mvn -pl md-sequencer spring-boot:run -Dspring-boot.run.profiles=dev
# or
java -jar md-sequencer/target/md-sequencer-exec.jar --spring.profiles.active=uat
```

## Configuration reference

Every setting lives under the `sequencer.*` prefix in
[`application.yml`](src/main/resources/application.yml), bound onto `SequencerProperties`. Nothing
is hardcoded in code — to point at a different NATS cluster, change block sizes, flip batching on
or off, etc., change config only.

### How to override a setting

Any of the standard Spring Boot mechanisms work, in increasing precedence:

- **Edit `application.yml`** directly for a permanent default.
- **A profile-specific file**, `application-{profile}.yml` on the classpath, activated with
  `--spring.profiles.active=<profile>` (or `SPRING_PROFILES_ACTIVE` env var) — e.g. an
  `application-staging.yml` overriding just `sequencer.ingress.nats.url`.
- **A mounted external file**: `java -jar md-sequencer.jar --spring.config.location=/workspace/config/application.yml` (this is what the `k8s/deployment.yaml` ConfigMap mount does).
- **Environment variables**, using Spring's relaxed binding (kebab-case key → upper-snake-case,
  dots → underscores): `sequencer.allocator.block-size` becomes
  `SEQUENCER_ALLOCATOR_BLOCK_SIZE=500000`.
- **JVM system properties**: `-Dsequencer.egress.backpressure=drop`.
- **Command-line arguments** (highest precedence): `java -jar md-sequencer.jar --sequencer.stamping.profile=v3`.

List-valued settings (`timestamp-template-ids`) can be overridden via env var with indexed keys
(`SEQUENCER_STAMPING_TIMESTAMP_TEMPLATE_IDS_0=1`, `_1=9`) or by overriding the whole list in a
config file.

### `sequencer.stamping.*` — SBE offset contract

| Key | Default | Meaning |
|---|---|---|
| `profile` | `v4` | `v4`: stamp `sequenceId` and `sequenceTimestamp` unconditionally on every message. `v3`: stamp `sequenceId` always, but `sequenceTimestamp` only for templateIds in `timestamp-template-ids`, and never enrich MatchEventBoundary. |
| `sequence-id-offset` | `8` | Absolute byte offset the sequencer writes `sequenceId` (u64 LE) to. |
| `sequence-timestamp-offset` | `32` | Absolute byte offset the sequencer writes `sequenceTimestamp` (u64 LE, from the Agrona `OffsetEpochNanoClock`) to. |
| `schema-id` | `100` | Expected SBE `schemaId`. A mismatch drops the message (see `validate-schema-id-per-message`). |
| `schema-id-offset` | `4` | Absolute byte offset of `schemaId` in the SBE `messageHeader`. |
| `template-id-offset` | `2` | Absolute byte offset of `templateId` in the SBE `messageHeader`. |
| `validate-schema-id-per-message` | `false` | `false`: only the first message after (re)subscribe is checked. `true`: every message is checked (extra branch per message; only enable if you don't trust the upstream wiring). |
| `timestamp-template-ids` | `[1, 9]` | v3 profile only — which templateIds get `sequenceTimestamp` stamped. |

### `sequencer.stamping.event-enrichment.*` — MatchEventBoundary first/lastSequenceId

| Key | Default | Meaning |
|---|---|---|
| `enabled` | `true` | Whether to track and stamp `firstSequenceId`/`lastSequenceId` on `MatchEventBoundary` messages. Disable to save the per-message eventId read + map bookkeeping if you don't need atomic-event replay by sequence range. |
| `event-id-offset` | `40` | Absolute byte offset of `gcmHeader.eventId`. |
| `boundary-template-id` | `6` | SBE templateId of `MatchEventBoundary`. |
| `first-sequence-id-offset` | `56` | Absolute byte offset `firstSequenceId` is stamped at. |
| `last-sequence-id-offset` | `64` | Absolute byte offset `lastSequenceId` is stamped at. |
| `max-tracked-events` | `65536` | Cap on concurrently in-flight (not-yet-boundary-closed) eventIds tracked at once; once exceeded, new events are skipped (metered via `sequencer_event_tracking_evicted_total`) and their eventual boundary falls back to its own sequenceId for both fields. Raise if you expect more than 64k simultaneously open events across all interleaved sources. |

### `sequencer.allocator.*` — sequence block leasing

| Key | Default | Meaning |
|---|---|---|
| `block-size` | `1000000` | How many sequenceIds are leased per KV compare-and-swap. Larger = fewer KV round trips but a bigger gap on crash; smaller = tighter gap bound but more KV traffic. |
| `lease-ahead-fraction` | `0.8` | Fraction of the current block consumed before the *next* block is proactively leased in the background. Lower this if you see the hot path occasionally blocking on KV RTT at block boundaries (rare; only happens if the proactive lease hasn't completed by the time the block is exhausted). |
| `kv-bucket` | `sequencer-lease` | NATS JetStream KV bucket name holding the high-water mark. Created automatically on startup if missing. |
| `kv-key` | `high-water` | KV key within that bucket. |

### `sequencer.ingress.nats.*`

| Key | Default | Meaning |
|---|---|---|
| `url` | `nats://nats:4222` | NATS server URL for ingress. This is a generic in-cluster fallback — every `application-{env}.yml` overrides it with the real per-environment endpoint (see "Deployment environments" below). |
| `mode` | `jetstream` | `jetstream`: durable pull consumer, at-least-once, ack-after-egress-confirmed (see redelivery contract above). `core`: plain subscription, at-most-once, lower latency, no ack machinery. |
| `stream` | `MD_RAW` | JetStream stream name to consume from (jetstream mode only). |
| `subject` | `tick.sbe.>` | Subject (wildcard allowed) subscribed to. |
| `consumer` | `sequencer` | Named durable consumer (jetstream mode only) — also the server-side fencing layer, since JetStream rejects a second active puller on the same durable. |
| `batch-size` | `1000` | Messages per JetStream pull-fetch (jetstream mode only); amortizes fetch overhead. |

### `sequencer.egress.*`

| Key | Default | Meaning |
|---|---|---|
| `type` | `jetstream` | `core` or `jetstream` — see architecture notes above for the throughput/durability trade-off. |
| `backpressure` | `block` | `block`: stall the hot thread until the destination has room (propagates backpressure to ingress naturally). `drop`: never stall; count the drop; the sequenceId gap is the signal downstream. |
| `max-stall-ms` | `500` | Threshold past which a `block`-policy stall is treated as an alarm condition (`sequencer_backpressure_stall_seconds_total`); does not itself cap how long the stall can run — market-data consumers prefer a stall over silent loss. |
| `nats.url` | `nats://nats:4222` | NATS server URL for egress. Same generic fallback as ingress — overridden per environment. |
| `nats.stream` | `MD_SEQUENCED` | JetStream stream published to (jetstream egress only). |
| `nats.subject` | `md.sequenced` | Base publish subject. `.batch` is appended automatically when `batching.enabled` is true. |
| `jetstream.max-in-flight` | `8192` | Bounded async publish-ack window size (jetstream egress only). Raise for higher throughput at the cost of more messages "in flight, unconfirmed" at any instant; lower to bound worst-case redelivery blast radius on crash. |

### `sequencer.egress.batching.*`

| Key | Default | Meaning |
|---|---|---|
| `enabled` | `true` | Whether to pack messages into `MessageBatch` envelopes before publishing. Spec default is `true` for jetstream egress, `false` for core (core NATS already coalesces at the TCP level; app-level batching there mainly saves protocol framing overhead) — this key is a single global toggle, so set it explicitly per environment/egress-type combination. **Required to reach ~1M logical msgs/sec on JetStream.** |
| `max-messages` | `100` | Flush after this many packed messages. |
| `max-bytes` | `65536` | Flush after this many packed bytes. |
| `max-linger-micros` | `1000` | Flush after this long since the batch's first message, even if neither threshold above is hit. At 1M msg/sec a 100-message batch fills in ~100us, so this timer mostly only matters in quiet markets. |
| `flush-on-event-boundary` | `true` | Force-flush immediately after a `MatchEventBoundary` so one matching-engine event never straddles two batches. |

### `sequencer.heartbeat.*`

| Key | Default | Meaning |
|---|---|---|
| `enabled` | `true` | Whether the sequencer emits its own high-water-mark heartbeat. |
| `interval-ms` | `100` | Emission interval. Lower for faster quiet-market gap detection at the cost of a bit more traffic; this is also the worst-case delay before a lingering partial batch gets flushed on a fully idle stream. |
| `source-id` | `SEQR` | Value stamped into the heartbeat's `source` field (max 8 ASCII chars). |

### `sequencer.leadership.*` — Kubernetes Lease-based leader election

| Key | Default | Meaning |
|---|---|---|
| `lease-name` | `gcm-md-sequencer` | Name of the `coordination.k8s.io/v1` Lease object. Must match the `Role` in `k8s/rbac.yaml` if you rename it. |
| `lease-namespace` | `default` | Namespace the Lease lives in. Generic fallback — every `application-{env}.yml` sets this to the actual namespace it's deployed into. |
| `lease-duration-seconds` | `10` | How long a held lease stays valid without renewal. |
| `renew-deadline-seconds` | `7` | How long the current leader has to renew before giving up leadership. Must be less than `lease-duration-seconds`. |
| `retry-period-seconds` | `2` | How often the elector retries acquiring/renewing. Must be less than `renew-deadline-seconds`. |

### Management/actuator settings

Standard Spring Boot Actuator keys under `management.*` (already set sensibly in
`application.yml`): `management.endpoints.web.exposure.include` controls which endpoints are
exposed (`health`, `prometheus`, `metrics` by default), `management.endpoint.health.show-details`
controls how much detail `/actuator/health` returns. See the [Spring Boot Actuator
docs](https://docs.spring.io/spring-boot/reference/actuator/index.html) for the full set — these
are unmodified Spring Boot behavior, not sequencer-specific.

## Build & run

```
mvn -pl md-sequencer -am install       # build (also builds md-models-sbe)
mvn -pl md-sequencer test              # unit tests
mvn -pl md-sequencer spring-boot:run   # run locally (needs a reachable NATS + Kubernetes context)

# or, after building:
java -jar md-sequencer/target/md-sequencer-exec.jar --spring.config.location=/path/to/application.yml
```

Note the executable jar is `md-sequencer-exec.jar` (classifier `exec`) — the plain
`md-sequencer.jar` is the slim artifact other reactor modules (`sequencer-bench`) depend on;
Spring Boot's repackaged fat jar nests classes under `BOOT-INF/`, which isn't usable as a normal
Maven dependency.

## Module layout

- `config/` — `SequencerProperties` (the entire config tree) and `ServiceConfiguration` (every
  bean definition; no `@Autowired` anywhere, per project convention — all wiring is explicit
  constructor injection through `@Bean` factory methods).
- `core/` — `SequenceStamper`, `SequenceAllocator`, `SequencerPipeline`, `LeaderElection`.
- `ingress/`, `egress/` — pluggable channel interfaces and NATS implementations.
- `heartbeat/` — `HeartbeatEmitter`.
- `metrics/` — `SequencerMetrics`.
- `health/` — `SequencerHealthIndicator`.

## Benchmarking and load testing

- `sequencer-bench` — JMH benchmark isolating `SequenceStamper.stamp()` (target: low double-digit
  ns per the spec's acceptance criteria). Run with `java -jar sequencer-bench/target/sequencer-bench.jar StampBenchmark`.
- `sequencer-loadgen` — publishes canned SBE traffic at a configurable rate and reports observed
  egress throughput and end-to-end latency percentiles. Run it once per mode to build the
  core/jetstream × batching-on/off comparison table the spec calls for:

  ```
  java -jar sequencer-loadgen/target/sequencer-loadgen.jar \
      --url nats://localhost:4222 --rate 1000000 --duration-seconds 30 \
      --ingress-mode jetstream --batched true
  ```

  Flags: `--url`, `--ingress-subject`, `--egress-subject`, `--rate` (msg/sec), `--duration-seconds`,
  `--batched` (`true` to unpack `MessageBatch` envelopes when measuring egress), `--ingress-mode`
  (`core` or `jetstream`).

## Kubernetes

[`k8s/`](k8s/) is a Kustomize base + one overlay per deployment environment (local, dev, uat,
prod, prod-dr — see "Deployment environments" above): `k8s/base/core/` (Deployment, RBAC,
Service — Guaranteed QoS, health probes, a Role/RoleBinding scoped to the Lease object leader
election uses), plus `k8s/base/pdb/` and `k8s/base/observability/` (PodDisruptionBudget and
ServiceMonitor, included by every overlay except `local`). Each overlay sets the namespace,
rewrites the image, and generates a `gcm-md-sequencer-config` ConfigMap containing exactly
`SPRING_PROFILES_ACTIVE=<env>` — that one variable is what pulls in the right
`application-{env}.yml` from the table above. See [`k8s/README.md`](k8s/README.md) for the full
layout, the placeholder values (ECR account IDs, NATS DNS names) to fill in before deploying, and
per-environment apply commands.
