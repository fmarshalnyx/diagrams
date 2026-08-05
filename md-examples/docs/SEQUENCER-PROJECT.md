# GCM-MD Sequencer Service — Project Specification

Input specification for building the sequencer service. This document captures all
design decisions and their rationale; treat it as authoritative. Where it says
"config", the setting must appear in `application.yml` with the exact key shown in
§12.

---

## 1. Purpose

A standalone Java Spring Boot service, deployed in Kubernetes, that:

1. Receives SBE-encoded market data messages (schema: `md-models-sbe`, see
   `md-models-sbe-v4.xml` in this repo) from an ingress channel (initial impl: NATS).
2. Assigns a **globally monotonic, sequential sequenceId** to every message —
   gaps are permitted (failover, restarts), regression is never permitted.
3. Writes the sequenceId (and a sequence timestamp) **directly into the binary
   message at fixed byte offsets** — the sequencer never decodes SBE.
4. Publishes the stamped message to a pluggable destination channel (initial
   impls: core NATS and NATS JetStream, selectable via config).

This is market data infrastructure: **minimize per-message work, maximize
throughput**. Target: **1,000,000 msgs/sec sustained** (~1 µs/message budget on
the hot path). The sequencer's output — one totally ordered stream — is the
canonical record; downstream consumers detect gaps via sequenceId and recover
out-of-band.

## 2. Non-negotiable requirements

- sequenceId is monotonic and sequential across ALL messages, all instruments,
  all sources. Gaps allowed; duplicates and regression never.
- Single writer at all times (enforced; see §10).
- Hot path never decodes SBE, never allocates per-message beyond what the NATS
  client forces, never touches Spring.
- Both destination types (core NATS, JetStream) implemented and config-selectable.
- Timestamps via **Agrona `OffsetEpochNanoClock`** (epoch-nanos, calibrated) —
  NOT `System.nanoTime()` (not epoch-based) and not bare `currentTimeMillis()`.

## 3. Message schema contract

The service builds against schema **v4** (`md-models-sbe-v4.xml`, schemaId=100,
version=4, little-endian). Every message begins with the 8-byte SBE
`messageHeader` followed by the frozen 48-byte `gcmHeader` composite:

| Absolute offset | Field | Writer |
|---|---|---|
| 0–7 | SBE messageHeader (blockLength, templateId, schemaId, version — u16 each) | line handler |
| **8** | `sequenceId` (u64 LE) | **sequencer, ALWAYS** |
| 16 | `sourceTimestamp` (u64 LE) | line handler |
| 24 | `ingestTimestamp` (u64 LE) | line handler |
| **32** | `sequenceTimestamp` (u64 LE) | **sequencer, ALWAYS** |
| 40 | `eventId` (u64 LE; 0 = none) | line handler |
| 48 | `reserved1` (u64 LE, zero) | — |
| 56+ | message body (8-byte aligned) | line handler |

Additionally, for `MatchEventBoundary` (templateId=6) only, when the
event-enrichment feature is enabled (config):

| Absolute offset | Field | Writer |
|---|---|---|
| 56 | `firstSequenceId` (u64 LE) | sequencer |
| 64 | `lastSequenceId` (u64 LE) | sequencer |

`MessageBatch` (templateId=10) is created BY the sequencer post-stamping and is
never itself stamped.

**Externalize all offsets and template rules in config** (§12 `stamping` block)
— the code must not hardcode 8/32/56/64 or template id 6. Compile config to
primitives at startup (plain `int` offsets, `boolean` feature flags).

**Sanity guard:** verify `schemaId == 100` at offset 4. Mandatory on the first
message after every (re)subscribe; optionally per-message behind config flag
`stamping.validate-schema-id-per-message` (default false). On mismatch: log,
increment `sequencer_schema_mismatch_total`, drop the message, and if it is the
first message, fail readiness — this catches "wrong stream wired to ingress"
before corrupting foreign bytes.

**v3 compatibility mode (config `stamping.profile: v3`):** stamp offset 8
always; stamp offset 32 only for templateIds in `stamping.timestamp-template-ids`
(v3 values: [1, 9]); no boundary enrichment. Default profile: `v4`.

## 4. Architecture

```
NATS ingress ──> [dispatcher thread] ──> stamp(seq, ts) ──> [batcher?] ──> DestinationChannel ──> NATS egress
                        │
                        └── SequenceAllocator (block lease via NATS KV)
```

**Threading: single-threaded pipeline, zero handoffs.** Monotonic assignment
mandates one stamping thread; the fastest design lets that same thread do
everything. The NATS client's message-delivery thread receives the message,
stamps it, hands it to the (optional) batcher, and calls the async publish. No
queues, no context switches. The sequence counter is a plain `long` (no
atomics — single writer). Do NOT introduce a Disruptor/ring buffer up front;
only add one if benchmarks show publish-latency spikes require buffering.

**Stamping (hot path, entire logic):**

```java
// UnsafeBuffer wraps the byte[] delivered by the NATS client
buffer.putLong(seqIdOffset, seq++, LITTLE_ENDIAN);          // abs 8
buffer.putLong(seqTsOffset, nanoClock.nanoTime(), LITTLE_ENDIAN); // abs 32
if (eventEnrichmentEnabled && templateId(buffer) == boundaryTemplateId) {
    // read eventId at abs 40; stamp first/last seq at abs 56/64 (see §8)
}
destination.publish(buffer, length);
```

Use **Agrona** `UnsafeBuffer` for all binary access. SBE-generated codecs are
used ONLY in tests (§15), never on the hot path.

**Spring's role:** config binding (`@ConfigurationProperties`), bean wiring,
`SmartLifecycle` start/stop, Actuator health/metrics endpoints on a separate
thread. Zero Spring-managed code executes per message after `start()`.

**Allocation reality:** the nats-java client allocates a `byte[]` per inbound
message — full garbage-free is not achievable on NATS. Do not fight it; tune GC
instead (§14). Everything the sequencer itself does must be allocation-free in
steady state (reused batch buffer, primitive counters, no boxing, no lambdas
capturing on the hot path).

## 5. Ingress

Interface (bean):

```java
public interface IngressChannel {
    void start(MessageHandler handler);   // handler invoked on client thread
    void stop();                          // drain-stop, idempotent
}
@FunctionalInterface
public interface MessageHandler {
    void onMessage(byte[] data);          // may mutate in place
}
```

Initial implementation: `NatsSequencerIngress`.

- **JetStream pull consumer** with a **named durable** (config
  `ingress.nats.consumer`) — the server-side single-active-consumer constraint
  doubles as a fencing layer (§10).
- Batch fetches (`ingress.nats.batch-size`, default 1000) to amortize overhead.
- **Ack semantics: ack AFTER egress publish is confirmed** (batched acks are
  fine: ack the fetch batch once all its messages have cleared egress — for
  JetStream egress, once their publish acks arrived; for core NATS egress, once
  written to the connection). Crash between receive and publish ⇒ redelivery,
  not loss.
  - **Documented consequence:** a redelivered message receives a NEW sequenceId
    — a duplicate payload under a different sequence number. Downstream dedupe
    key: (`source`-scope) `instrumentId` + `sourceSeqNum`, which the schema
    carries. State this in the README; it surprises people.
- Also support plain core-NATS subscription mode (config `ingress.nats.mode:
  core|jetstream`) for fast-path inputs; core mode has no acks and therefore
  at-most-once ingress.

## 6. Sequence allocation — block leasing

Gaps are allowed, so per-message persistence is unnecessary. Use
**high-water-mark block leasing** backed by a NATS KV bucket:

- KV key holds "leased up to N".
- At startup/failover: read N, lease `[N+1, N+blockSize]` by writing
  `N+blockSize` (compare-and-swap on KV revision), start issuing at `N+1`.
- When the local counter exhausts the block, lease the next one. With
  `allocator.block-size: 1_000_000` that's ~1 KV write/sec at full rate.
- Crash cost: at most one block of gap. sequenceId remains strictly monotonic
  across restarts with zero hot-path persistence.
- Lease the next block PROACTIVELY (e.g. at 80% consumption, on the same thread
  between messages or via a completed-future check) so block rollover never
  stalls the hot path waiting on KV RTT.
- Every KV block-lease is also the **fencing checkpoint**: verify leadership
  lease validity before writing (§10).

## 7. Egress

```java
public interface DestinationChannel {
    void publish(DirectBuffer buffer, int offset, int length); // hot path
    void flush();                                              // batcher/linger
    void stop();                                               // drain + close
}
```

Two implementations, selected by `egress.type`:

**`CoreNatsDestination`** — fire-and-forget on one connection (order
preserved). No persistence: loss is possible but immediately *detectable*
downstream via sequenceId gaps; recovery is an out-of-band replay service's job
(classic market-data fast path). Monitor the client's outbound buffer; a full
buffer engages the backpressure policy (§9). Easily exceeds 1M msgs/sec.

**`JetStreamDestination`** — async publish with a **bounded in-flight ack
window** (`egress.jetstream.max-in-flight`, default 8192). Ack futures are
reaped off the hot thread (completion callbacks decrement a counter; failures
increment metrics and, per config, trigger republish-or-halt). Publish order on
one connection preserves stream order. Realistic single-publisher ceiling
without batching: ~200–600k msgs/sec on file-storage R1 — **batching (§8) is
required to reach 1M logical msgs/sec on JetStream**.

Egress subject model: the sequenced output is a **single firehose subject**
(`md.sequenced` unbatched, `md.sequenced.batch` batched). Per-symbol filtered
distribution is explicitly OUT OF SCOPE — it belongs to a downstream fan-out
tier. Total order is meaningless if consumers cherry-pick subjects with
independent delivery.

## 8. Batching (decorator) and event handling

`BatchingDestination implements DestinationChannel`, wrapping either concrete
destination. Config-enabled; **default ON for JetStream, OFF for core NATS**
(core NATS already coalesces at TCP level; app-level batching there saves
~20–30% protocol framing overhead — let the benchmark harness decide).

- Packs stamped messages into a **`MessageBatch` (templateId=10) envelope**:
  `firstSequenceId`, `lastSequenceId`, `batchTimestamp`, `flags`, then a group
  of length-prefixed complete-message blobs in sequence order. Write the
  envelope with raw `UnsafeBuffer` ops against one reusable direct buffer
  (memcpy each message in, publish, reset) — allocation-free steady state.
- **Flush policy:** flush when `max-messages` OR `max-bytes` OR
  `max-linger-micros` is hit, whichever first. At 1M msgs/sec a 100-message
  batch fills in ~100 µs, so the linger timer effectively only fires in quiet
  markets — added latency under load is ~50 µs median.
- **`flush-on-event-boundary` (default true):** flush immediately after a
  MatchEventBoundary so a matching-engine event never straddles batches; set
  envelope `flags` bit0.
- The linger timer and the heartbeat (below) may share a scheduler; the flush
  itself must execute on/coordinate with the stamping thread (simplest: the
  timer sets a volatile flag; the stamping thread checks it per message; a
  truly idle stream is flushed by the heartbeat path).

**Heartbeat (sequencer-emitted).** Every `heartbeat.interval-ms` (default 100),
emit a schema `Heartbeat` (templateId=4) with `heartbeatType=SEQUENCER`,
`source="SEQR"`, `highWaterSequenceId` = last assigned seq, stamped and
published through the normal pipeline (it consumes a sequenceId like any
message). This makes gap detection sound in quiet markets — "no data" vs
"missing data" — and provides sequencer liveness independent of K8s probes.

**MatchEventBoundary enrichment** (config `stamping.event-enrichment.enabled`,
default true): read `eventId` at abs offset 40 on every message; when non-zero
and it's the first sighting, record `eventId → firstSeq` (bounded open-address
long→long map, e.g. Agrona `Long2LongHashMap`, with size cap + eviction
metrics); when templateId==6, stamp `firstSequenceId` (abs 56) and
`lastSequenceId` (abs 64, = previous message's seq for that event) and remove
the entry. Multiple sources interleave in the global stream, which is exactly
why tracking is per-eventId, not "messages since last boundary".

## 9. Backpressure policy

Config `egress.backpressure: block | drop` (default **block**).

- **block:** when the JetStream in-flight window is full (or the core-NATS
  outbound buffer is full), the stamping thread waits (bounded spin-then-park);
  backpressure propagates naturally to the JetStream ingress consumer (unacked,
  unfetched). Emit `sequencer_backpressure_stall_seconds` and alarm past
  `egress.max-stall-ms`. Rationale: market-data consumers prefer a brief stall
  over silent, unreplayable loss.
- **drop:** never stall; drop the message, count it
  (`sequencer_dropped_total`), continue. Note: the sequenceId is still
  consumed ⇒ downstream sees it as a gap, which is the correct signal.

## 10. Single-writer enforcement, fencing, lifecycle ordering

- Deployment: **single replica active + warm standby**, Kubernetes
  **Lease-based leader election** (fabric8 leader-election or Spring Cloud
  Kubernetes). Standby holds connections warm but does not subscribe.
- **Startup/failover ordering (strict):**
  1. Connect + verify egress (and KV bucket exists).
  2. Acquire leadership lease.
  3. Lease sequence block from KV.
  4. Subscribe ingress.
  Never reverse — do not consume what you cannot publish. Readiness = all four.
- **On lease loss:** drain-stop ingress FIRST, then stop publishing, drop
  readiness. A wedged old leader is fenced two ways: (a) leadership-lease
  validity is checked before every KV block lease (natural fencing point, ~1/sec
  at full rate), (b) the JetStream named durable consumer rejects a second
  active puller.
- Graceful shutdown (SIGTERM): stop ingress → flush batcher → await in-flight
  acks (bounded) → final KV lease write is NOT needed (block already leased) →
  release leadership → exit. Set `terminationGracePeriodSeconds` accordingly.

## 11. Module / package layout

```
gcm-md-sequencer/
├── build.gradle.kts                  (or Maven; Java 21+)
├── schema/md-models-sbe-v4.xml       # canonical schema copy
├── sequencer-service/
│   └── src/main/java/com/usb/gcm/md/sequencer/
│       ├── SequencerApplication.java
│       ├── config/SequencerProperties.java        # @ConfigurationProperties("sequencer")
│       ├── core/
│       │   ├── SequenceAllocator.java             # block leasing (KV)
│       │   ├── SequenceStamper.java               # offset stores + event enrichment
│       │   ├── SequencerPipeline.java             # SmartLifecycle; owns ordering §10
│       │   └── LeaderElection.java
│       ├── ingress/
│       │   ├── IngressChannel.java
│       │   └── NatsSequencerIngress.java
│       ├── egress/
│       │   ├── DestinationChannel.java
│       │   ├── CoreNatsDestination.java
│       │   ├── JetStreamDestination.java
│       │   └── BatchingDestination.java           # decorator + MessageBatch writer
│       ├── heartbeat/HeartbeatEmitter.java
│       └── metrics/SequencerMetrics.java
├── sequencer-loadgen/                 # load generator module (§15)
└── sequencer-bench/                   # JMH module (§15)
```

Dependencies: `org.agrona:agrona`, `io.nats:jnats`, Spring Boot (web excluded;
actuator on), Micrometer + Prometheus registry, SBE tool (`uk.co.real-logic:sbe-tool`)
as codegen for TESTS only, JMH in bench module, fabric8 kubernetes-client
(leader election). Generate SBE codecs from `schema/` in the build.

## 12. Configuration (`application.yml`)

```yaml
sequencer:
  stamping:
    profile: v4                       # v4 | v3
    sequence-id-offset: 8
    sequence-timestamp-offset: 32
    schema-id: 100
    schema-id-offset: 4
    template-id-offset: 2
    validate-schema-id-per-message: false
    event-enrichment:
      enabled: true
      event-id-offset: 40
      boundary-template-id: 6
      first-sequence-id-offset: 56
      last-sequence-id-offset: 64
      max-tracked-events: 65536
    # v3 profile only:
    timestamp-template-ids: [1, 9]
  allocator:
    block-size: 1000000
    lease-ahead-fraction: 0.8
    kv-bucket: sequencer-lease
    kv-key: high-water
  ingress:
    nats:
      url: nats://nats.market-data.svc:4222
      mode: jetstream                 # jetstream | core
      stream: MD_RAW
      subject: "tick.sbe.>"
      consumer: sequencer             # named durable
      batch-size: 1000
  egress:
    type: jetstream                   # core | jetstream
    backpressure: block               # block | drop
    max-stall-ms: 500
    nats:
      url: nats://nats.market-data.svc:4222
      stream: MD_SEQUENCED
      subject: md.sequenced           # .batch appended automatically when batching on
    jetstream:
      max-in-flight: 8192
    batching:
      enabled: true                   # default true for jetstream, false for core
      max-messages: 100
      max-bytes: 65536
      max-linger-micros: 1000
      flush-on-event-boundary: true
  heartbeat:
    enabled: true
    interval-ms: 100
    source-id: SEQR
  leadership:
    lease-name: gcm-md-sequencer
    lease-namespace: market-data
    lease-duration-seconds: 10
    renew-deadline-seconds: 7
```

## 13. Observability

Micrometer/Prometheus. Hot-thread rules: counters via single-writer plain
fields exposed as gauges or `LongAdder`; NEVER a blocking or allocating metric
call per message.

- `sequencer_messages_total`, `sequencer_bytes_total`
- `sequencer_current_sequence_id` (gauge)
- `sequencer_batches_total`, batch-size distribution (sampled)
- `sequencer_inflight_window` (gauge), `sequencer_publish_failures_total`
- `sequencer_backpressure_stall_seconds_total`, `sequencer_dropped_total`
- `sequencer_blocks_leased_total`, `sequencer_lease_failures_total`
- `sequencer_schema_mismatch_total`
- `sequencer_source_seq_gap_total{source}` — per-source `sourceSeqNum` gap
  counter: distinguishes data lost UPSTREAM of the sequencer from data lost in
  it. (Reads instrumentId+sourceSeqNum at fixed offsets per template — make
  this a config-gated feature since offsets are template-specific; default on
  for templateId 9.)
- Latency: HdrHistogram of receive→publish, recorded via a single-writer ring
  drained off-thread; export p50/p99/p99.9/max.
- Actuator health: components = ingress connected, egress connected, leadership
  held, allocator healthy. Readiness per §10.

## 14. JVM & Kubernetes

- Java 21+. GC: `-XX:+UseZGC -XX:+ZGenerational -XX:+AlwaysPreTouch`, sized
  heap (start 4–8 GB), because jnats allocates per message.
- K8s: **Guaranteed QoS** (requests == limits), node pool with
  `cpuManagerPolicy: static`, 2–4 exclusive cores (hot thread + GC +
  housekeeping). No CPU limits throttling surprises.
- Actuator on its own port/thread pool. Liveness = process up; readiness = §10.
- `terminationGracePeriodSeconds` ≥ drain time (default 30).
- Manifests to include: Deployment (replicas: 2, leader-elected),
  Role/RoleBinding for Lease, ServiceMonitor, PDB.

## 15. Testing & benchmarks

**Unit/integration (SBE codecs allowed here):**
- Offset-contract test: encode each of the 9 data templates with the generated
  v4 codecs, run the stamper against raw bytes, decode with codecs, assert
  sequenceId/sequenceTimestamp landed in the right named fields. This test is
  the proof that config offsets match the schema — it must fail if either
  drifts.
- v3-profile test with v3-layout fixtures (offset 8 always; 32 only for
  templates 1, 9).
- MatchEventBoundary enrichment: interleave two sources' events; assert
  first/last stamped correctly per eventId.
- MessageBatch round-trip: batch N stamped messages, decode envelope with
  codecs, verify order, first/last seq, blob integrity.
- Allocator: block exhaustion, proactive lease, restart-resume (monotonicity
  across simulated crash), KV CAS conflict (split-brain attempt loses).
- Testcontainers NATS: full pipeline end-to-end for both egress types, ack-
  after-publish redelivery behavior, drain-stop.

**Benchmarks (must be re-runnable after every dependency bump):**
- JMH: stamp path in isolation (target: low double-digit ns).
- `sequencer-loadgen`: publishes canned SBE messages at a configurable rate to
  the ingress stream; measures sustained sequenced throughput and
  receive→publish latency distribution. Modes to compare: core vs jetstream ×
  batching on/off. Acceptance: 1M msgs/sec sustained with batching+jetstream on
  reference hardware; document the numbers achieved.

## 16. Acceptance criteria

1. Both destinations selectable purely by config; batching decorator togglable.
2. Offset-contract test green against generated v4 codecs; v3 profile test green.
3. Monotonicity across kill -9 + restart and across leader failover
   (integration test: no regression, gap ≤ block-size, no duplicate seq).
4. Ack-after-publish verified: crash injection between receive and publish
   yields redelivery with a new seq (and the dedupe contract is documented).
5. Sequencer heartbeat visible on egress with correct high-water mark.
6. Load test demonstrates target throughput with the jetstream+batching
   configuration; report includes latency percentiles.
7. Zero per-message allocation attributable to sequencer code (verify with
   allocation profiling; jnats-internal allocation is exempt and documented).

## 17. Out of scope / open items

- Per-symbol fan-out tier (downstream service).
- Replay/recovery service answering gap requests (downstream; JetStream egress
  stream is its data source).
- instrumentId assignment authority (line handler / refdata service concern —
  the sequencer never reads it except for the optional gap metric).
- Sharded sequencers (gcmHeader.reserved1 is reserved for a partition/epoch id
  if this ever happens).
- v3→v4 coordinated rollout plan for line handlers and consumers.
