# sequencer-bench

JMH microbenchmark module for `md-sequencer`. **Not required to build, deploy, or run the
service** — it's a development/validation tool used to prove the hot-path stamping logic meets
the project spec's performance target (`SEQUENCER-PROJECT.md` §4, §15: "JMH: stamp path in
isolation, target: low double-digit ns"). Nothing in `md-sequencer` depends on this module; the
dependency runs one direction only (this module depends on `md-sequencer` as a library, to call
into `SequenceStamper` directly).

## What it measures

`StampBenchmark` isolates exactly one thing: `SequenceStamper.stamp(buffer, sequenceId)` — the
entire per-message hot-path logic (schemaId guard, sequenceId/sequenceTimestamp writes, and
MatchEventBoundary enrichment bookkeeping). There is no NATS client, no Spring context, no
allocator, no I/O of any kind in the timed region — the goal is to measure the stamping logic in
complete isolation from everything around it, since that's the piece the spec puts a hard
per-message time budget on (the service's overall 1,000,000 msgs/sec target implies roughly a 1us
budget per message across the *entire* pipeline; this benchmark isolates just the stamping slice
of that budget).

The benchmark:
- builds one canned `MarketDataDelta` message once, in `@Setup`, using the generated SBE codec
  (codecs are for test/bench setup only, per the project's "never decode SBE on the hot path"
  rule — the timed region only ever touches raw offsets)
- repeatedly calls `stamper.stamp(buffer, ++sequenceId)` against that same reused buffer
- runs in `AverageTime` mode, reporting nanoseconds/op, with a 5x1s warmup and 5x1s measurement
  phase, single JVM fork

Since it uses `SequenceStamper` with default `SequencerProperties()` (v4 profile, event
enrichment enabled), the timed path includes the schemaId check, both fixed-offset writes, and
the MatchEventBoundary bookkeeping branch (which is a no-op read+compare for a non-boundary
message, as the canned `MarketDataDelta` fixture is).

## When to run it

- After any change to `SequenceStamper` or the `OffsetEpochNanoClock`/`SequencerMetrics` calls it
  makes, to confirm you haven't regressed the per-message cost.
- After a dependency bump (Agrona, JDK version) — the project spec calls for benchmarks to be
  re-runnable after every dependency bump.
- Never needed just to run `md-sequencer` in any environment (local, staging, production).

## Build & run

```
mvn -pl sequencer-bench -am install         # builds md-models-sbe + md-sequencer + this module
java -jar sequencer-bench/target/sequencer-bench.jar StampBenchmark
```

`target/sequencer-bench.jar` is a shaded/executable jar (main class `org.openjdk.jmh.Main`); the
argument is a regex matched against benchmark class names, so `StampBenchmark` runs just that
class. Run with no argument to list all discovered benchmarks, or pass JMH's own flags after the
regex, e.g.:

```
java -jar sequencer-bench/target/sequencer-bench.jar StampBenchmark -f 2 -wi 10 -i 10
```

(`-f` fork count, `-wi`/`-i` warmup/measurement iteration counts — these override the
`@Fork`/`@Warmup`/`@Measurement` annotation defaults in `StampBenchmark`.)

## Reading the output

JMH prints a per-iteration and final summary table, e.g.:

```
Benchmark               Mode  Cnt   Score   Error  Units
StampBenchmark.stamp    avgt    5  18.432 ± 0.911  ns/op
```

`Score` is the average nanoseconds per `stamp()` call; `Error` is the 99.9% confidence interval
half-width. Compare `Score` against the spec's low-double-digit-ns target — if it creeps
noticeably above that, look for: a newly-introduced allocation in the timed path (check with
`-prof gc`), a widened `HashSet`/`Long2LongHashMap` lookup cost from `max-tracked-events` growth,
or JIT warmup not fully settling (try raising `-wi`).

## Adding more benchmarks

Add a new `@State(Scope.Thread)`-annotated class with `@Benchmark` methods under
`gcm.md.sequencer.bench`, following `StampBenchmark`'s pattern: build fixtures once in
`@Setup(Level.Trial)`, keep the `@Benchmark` method itself allocation-free, and consume the return
value via `Blackhole` so the JIT can't dead-code-eliminate the call. No changes to `pom.xml` are
needed — the JMH annotation processor picks up any new benchmark class automatically at compile
time.
