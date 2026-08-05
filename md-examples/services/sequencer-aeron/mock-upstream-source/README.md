# mock-upstream-source

A persistent Spring Boot service, deployable independently of
`services/sequencer-aeron/line-handler-template`, replacing `sequencer-loadgen`
(implementation-steps.md Milestone 5B — `loadgen`'s actual retirement is 5B.4, gated on
`deploy/local/40-smoke-test.sh`/`50-failover-drill.sh` switching over to this service first, not
done as part of this module).

## How it differs from `loadgen`

Two ways. First, it sits one layer further upstream: `loadgen` published straight at the
sequencer's own ingress subject; this service publishes to a subject standing in for whatever
real upstream feed a line handler would consume, and `line-handler-template` relays it the rest
of the way — though you can still point it directly at a sequencer's own ingress subject if you
don't need the line-handler hop.

Second, and more fundamentally: `loadgen` was a one-shot CLI tool (`--duration-seconds`, then
exit). This is a **continuous, always-on service** — it starts generating and verifying traffic
the moment it comes up, and keeps doing so for as long as it's deployed. There's no in-process
timer; scale the Deployment to `0` replicas to turn it off, the same way you would any other
service in this reactor. It also deliberately injects gaps and duplicates
(`gap-probability`/`duplicate-probability`) to continuously exercise the sequencer's dedup path,
which `loadgen` never did, and reports everything as Prometheus metrics rather than a one-shot
exit code.

## What it does

1. **Generate** (`gcm.md.mockupstreamsource.generate`): publishes synthetic `MarketDataDelta`
   SBE messages (built via the generated codec, same as `loadgen`) to `subject` at `rate`,
   paced per `pattern` (`steady` or `bursty`), continuously. Independently of pattern, each tick
   can be a deliberate gap (skipped `sourceSeqNum`, never published) or a deliberate duplicate
   (published twice, byte-identical) per `gap-probability`/`duplicate-probability` — `seed` makes
   a given run's decisions reproducible.
2. **Verify** (`gcm.md.mockupstreamsource.verify`): subscribes to `egress-subject` (the
   sequencer's final observed output) for the lifetime of the service and checks
   contiguity/no-duplicates on the observed `sequenceId`s.

## Metrics

Exposed at `/actuator/prometheus`:

| Metric | Meaning |
|---|---|
| `mock_upstream_published_total` | Messages actually published upstream (including deliberate duplicates, excluding skips). |
| `mock_upstream_observed_total` | Messages observed on the final egress subject. |
| `mock_upstream_gap_total` | Sequence gaps detected in observed egress. **Should be permanently zero** — the sequencer's own dedup is supposed to absorb every deliberately-injected gap this service generates (a gap here never contributes a `sequenceId` at all). A nonzero reading is a real sequencer bug, not routine noise (same contract as `bridge_gap_total` elsewhere in this reactor). |
| `mock_upstream_duplicate_total` | Sequence duplicates detected in observed egress. Same "should be permanently zero" contract — the sequencer's dedup is supposed to absorb every deliberate duplicate this service sends. |

## Prerequisites

- A reachable NATS server **with JetStream enabled — mandatory**, unlike `loadgen`'s optional
  `--ingress-mode`. `line-handler-template`'s crash-recovery story depends on durable-consumer
  redelivery having something to replay from, which only JetStream provides.
- Something consuming `subject` and relaying it onward (a `line-handler-template` instance, or
  point `subject` directly at whatever the sequencer itself consumes if you're bypassing the
  line-handler hop).

## Build & run

```
mvn -pl services/sequencer-aeron/mock-upstream-source -am install
java -jar services/sequencer-aeron/mock-upstream-source/target/mock-upstream-source-exec.jar
```

Config lives in `application.yml` (`mock-upstream-source.*`) or any Spring Boot config override
mechanism (env vars, `--mock-upstream-source.rate=5000`, etc.) — see
`src/main/resources/application.yml` for the full tree and defaults.

| Key | Default | Meaning |
|---|---|---|
| `url` | `nats://localhost:4222` | NATS server URL. |
| `stream` | `MOCK_UPSTREAM` | JetStream stream name; created idempotently on startup if it doesn't exist. |
| `subject` | `upstream.mock.marketdata` | Subject published to (what a line handler consumes from). |
| `egress-subject` | `md.sequenced` | Subject to verify final sequenced output against. |
| `rate` | `100000` | Target publish rate, messages/sec (peak rate during burst windows if `pattern=bursty`). |
| `pattern` | `steady` | `steady` (constant interval) or `bursty` (alternating full-rate/quiet windows). |
| `gap-probability` | `0.0` | Per-tick probability of a deliberate, un-sent gap in `sourceSeqNum`. |
| `duplicate-probability` | `0.0` | Per-tick probability of a deliberate byte-identical re-publish. |
| `seed` | `0` | RNG seed for gap/duplicate decisions. |
| `batched` | `false` | Whether to expect `MessageBatch` envelopes on the egress subject. |
