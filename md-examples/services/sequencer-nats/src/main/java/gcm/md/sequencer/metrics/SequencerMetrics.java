package gcm.md.sequencer.metrics;

import gcm.md.sequencer.egress.EgressMetrics;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import org.HdrHistogram.Histogram;
import org.HdrHistogram.Recorder;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

/**
 * All sequencer observability in one place. Hot-thread-touched fields are either plain
 * {@code long}s (single writer: the stamping thread) exposed as gauges, or {@link LongAdder}s
 * where a value can also be written from an async NATS callback thread (e.g. publish failures).
 * Nothing here performs a blocking or allocating call from the hot path itself.
 *
 * <p>Implements {@link EgressMetrics} so {@code libs/nats-egress} destinations (which must not
 * depend on Micrometer) can report through this same bean (design §3.2, §4 listener pattern).
 */
public class SequencerMetrics implements EgressMetrics {

    private final MeterRegistry registry;

    // Single-writer (stamping thread) plain counters, exposed as gauges.
    private volatile long messagesTotal;
    private volatile long bytesTotal;
    private volatile long currentSequenceId;
    private volatile long batchesTotal;
    private volatile int inflightWindow;

    // Cross-thread counters (async ack/callback threads may increment these).
    private final LongAdder publishFailuresTotal = new LongAdder();
    private final LongAdder droppedTotal = new LongAdder();
    private final LongAdder blocksLeasedTotal = new LongAdder();
    private final LongAdder leaseFailuresTotal = new LongAdder();
    private final LongAdder schemaMismatchTotal = new LongAdder();
    private final LongAdder backpressureStallNanosTotal = new LongAdder();
    private final LongAdder eventTrackingEvictedTotal = new LongAdder();

    private final DistributionSummary batchSizeDistribution;
    private final ConcurrentHashMap<String, Counter> sourceSeqGapCounters = new ConcurrentHashMap<>();

    // Off-thread-drained latency histogram: hot thread only calls recordValue (wait-free).
    private final Recorder latencyRecorder = new Recorder(TimeUnit.SECONDS.toNanos(10), 3);
    private final ScheduledExecutorService histogramDrainer =
            Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "sequencer-latency-drain");
                t.setDaemon(true);
                return t;
            });

    private volatile double latencyP50Micros;
    private volatile double latencyP99Micros;
    private volatile double latencyP999Micros;
    private volatile double latencyMaxMicros;

    /**
     * Creates the metrics bean, registering all gauges/counters against the given registry
     * and starting the off-thread latency histogram drain.
     */
    public SequencerMetrics(MeterRegistry registry) {
        this.registry = registry;
        registry.gauge("sequencer_messages_total", this, m -> m.messagesTotal);
        registry.gauge("sequencer_bytes_total", this, m -> m.bytesTotal);
        registry.gauge("sequencer_current_sequence_id", this, m -> m.currentSequenceId);
        registry.gauge("sequencer_batches_total", this, m -> m.batchesTotal);
        registry.gauge("sequencer_inflight_window", this, m -> m.inflightWindow);
        registry.gauge("sequencer_publish_failures_total", publishFailuresTotal, LongAdder::sum);
        registry.gauge("sequencer_dropped_total", droppedTotal, LongAdder::sum);
        registry.gauge("sequencer_blocks_leased_total", blocksLeasedTotal, LongAdder::sum);
        registry.gauge("sequencer_lease_failures_total", leaseFailuresTotal, LongAdder::sum);
        registry.gauge("sequencer_schema_mismatch_total", schemaMismatchTotal, LongAdder::sum);
        registry.gauge("sequencer_event_tracking_evicted_total", eventTrackingEvictedTotal, LongAdder::sum);
        registry.gauge("sequencer_backpressure_stall_seconds_total", backpressureStallNanosTotal,
                adder -> adder.sum() / 1_000_000_000.0);
        registry.gauge("sequencer_latency_p50_micros", this, m -> m.latencyP50Micros);
        registry.gauge("sequencer_latency_p99_micros", this, m -> m.latencyP99Micros);
        registry.gauge("sequencer_latency_p999_micros", this, m -> m.latencyP999Micros);
        registry.gauge("sequencer_latency_max_micros", this, m -> m.latencyMaxMicros);
        this.batchSizeDistribution = DistributionSummary.builder("sequencer_batch_size")
                .description("Distribution of MessageBatch message counts")
                .register(registry);

        histogramDrainer.scheduleAtFixedRate(this::drainLatencyHistogram, 1, 1, TimeUnit.SECONDS);
    }

    /** Records one stamped-and-published message on the hot path (plain-field increment only). */
    public void onMessagePublished(int bytes, long sequenceId) {
        messagesTotal++;
        bytesTotal += bytes;
        currentSequenceId = sequenceId;
    }

    /** Records a flushed batch's size on the hot/batcher path. */
    @Override
    public void onBatchFlushed(int messageCount) {
        batchesTotal++;
        batchSizeDistribution.record(messageCount);
    }

    /** Updates the current JetStream in-flight ack window occupancy (hot path, plain field). */
    @Override
    public void setInflightWindow(int inflight) {
        this.inflightWindow = inflight;
    }

    /** Wait-free hot-path record of one message's receive-to-publish latency, in nanoseconds. */
    public void recordLatencyNanos(long nanos) {
        latencyRecorder.recordValue(Math.min(nanos, TimeUnit.SECONDS.toNanos(10) - 1));
    }

    /** Increments the publish-failure counter; safe to call from an async ack-callback thread. */
    @Override
    public void incrementPublishFailures() {
        publishFailuresTotal.increment();
    }

    /** Increments the dropped-message counter under the {@code drop} backpressure policy. */
    @Override
    public void incrementDropped() {
        droppedTotal.increment();
    }

    /** Records a completed backpressure stall of the given duration, in nanoseconds. */
    @Override
    public void recordBackpressureStall(long nanos) {
        backpressureStallNanosTotal.add(nanos);
    }

    /** Increments the successful block-lease counter. */
    public void incrementBlocksLeased() {
        blocksLeasedTotal.increment();
    }

    /** Increments the KV CAS lease-failure counter (split-brain contention). */
    public void incrementLeaseFailures() {
        leaseFailuresTotal.increment();
    }

    /** Increments the schemaId sanity-guard mismatch counter. */
    public void incrementSchemaMismatch() {
        schemaMismatchTotal.increment();
    }

    /** Increments the counter for MatchEventBoundary tracking skipped due to the max-tracked-events cap. */
    public void incrementEventTrackingEvicted() {
        eventTrackingEvictedTotal.increment();
    }

    /** Increments the per-source sourceSeqNum gap counter (upstream-of-sequencer loss detection). */
    public void incrementSourceSeqGap(String source) {
        sourceSeqGapCounters
                .computeIfAbsent(source, s -> Counter.builder("sequencer_source_seq_gap_total")
                        .tag("source", s)
                        .register(registry))
                .increment();
    }

    private void drainLatencyHistogram() {
        Histogram histogram = latencyRecorder.getIntervalHistogram();
        if (histogram.getTotalCount() == 0) {
            return;
        }
        latencyP50Micros = histogram.getValueAtPercentile(50.0) / 1000.0;
        latencyP99Micros = histogram.getValueAtPercentile(99.0) / 1000.0;
        latencyP999Micros = histogram.getValueAtPercentile(99.9) / 1000.0;
        latencyMaxMicros = histogram.getMaxValue() / 1000.0;
    }

    /** Stops the off-thread histogram drainer; call on service shutdown. */
    public void stop() {
        histogramDrainer.shutdownNow();
    }
}
