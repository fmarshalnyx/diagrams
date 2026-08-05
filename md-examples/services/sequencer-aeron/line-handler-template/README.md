# line-handler-template

A reference implementation, not a production service. It exists so a real line-handler author
has something concrete to copy instead of just `libs/ingress-transport`'s interface and Javadoc.
See `docs/AERON-SEQUENCER-IMPLEMENTATION-STEPS.md`'s Milestone 5B for why this replaced the
originally-planned `ingress-shim` strangler service.

## What it does

Consumes a durable JetStream pull subscription (by default, pointed at Milestone 5B's
`mock-upstream-source`), stamps each message's `sourceSeqNum`, and relays it into the sequencer
via whichever `IngressTransport` `line-handler.ingress-transport` selects:

- `aeron` (default) — `libs/cluster-client`'s `ClusterIngressClient`, talking directly to the
  Aeron cluster.
- `nats` — `libs/nats-egress`'s `NatsIngressTransport`, publishing to phase-1's NATS ingress
  subject instead.

Switching transports is a one-line config change (`line-handler.ingress-transport`); nothing
else about this service's code changes.

## Replace this for your real feed

`UpstreamRelay`'s **upstream-consumption half only** — the `subscription.fetch(...)` loop in
`run()`/`onFetched`. Whatever your real feed's protocol is, your replacement must preserve two
properties or the crash-recovery story below breaks:

1. **`sourceSeqNum` must be derived from something that naturally survives a crash and replays
   identically on redelivery** — a native feed sequence number, or whatever durable-position
   mechanism your real protocol offers. This template uses JetStream's own
   `Message.metaData().streamSequence()` because that's exactly this property for a JetStream
   feed; your feed's equivalent will look different, but must have the same characteristic.
2. **Never acknowledge/commit upstream progress before `IngressTransport#offer` returns `>= 0`.**
   See `onFetched`'s ordering — ack only follows a successful offer, never precedes it.

## Keep as-is

- `ServiceConfiguration`'s transport selection (`@ConditionalOnProperty` wiring) — this is the
  part every line handler needs identically, regardless of upstream protocol.
- `UpstreamRelay#offerWithRetry` — the retry/backoff loop against a negative `offer()` result.
  Never blocks indefinitely; always responsive to shutdown.
- The "don't ack until offered" ordering itself (distinct from *how* you fetch/ack upstream,
  which is protocol-specific).
- `LineHandlerMetrics` wiring, for the same observability shape across every line handler.

## Crash-recovery demo

No local file, database, or counter anywhere in this service tracks progress. Try it:

1. Run this service pointed at a running cluster and `mock-upstream-source` in `generate` mode.
2. `kill -9` this service mid-burst.
3. Restart it.

JetStream's durable-consumer state redelivers the unacked tail with identical
`streamSequence()`s, which get identical `sourceSeqNum`s stamped again on re-offer. The
sequencer recognizes them as already-seen (`Verdict.DUPLICATE`) and safely no-ops — see
`libs/cluster-client`'s package-info for the full idempotency contract this relies on
("republish your tail since last known-processed").

## Config reference

See `src/main/resources/application.yml` for the full `line-handler.*` tree and defaults
(`upstream.*` — always used; `aeron.*` — used when `ingress-transport: aeron`; `nats-ingress.*`
— used when `ingress-transport: nats`; `stamping.source-seq-num-offset` — must match the
sequencer's own `StampingConfig` for whichever templateId your messages use; `offer.*` — retry
tuning).

## Not yet wired into automation

This service isn't yet plugged into `deploy/local/40-smoke-test.sh` or `50-failover-drill.sh` —
that's Milestone 5B.3, not done here.
