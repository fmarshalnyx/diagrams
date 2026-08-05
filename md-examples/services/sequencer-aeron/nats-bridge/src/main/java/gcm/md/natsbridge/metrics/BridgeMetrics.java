package gcm.md.natsbridge.metrics;

import gcm.md.sequencer.egress.EgressMetrics;
import io.micrometer.core.instrument.MeterRegistry;

import java.util.concurrent.atomic.LongAdder;

/**
 * Bridge observability (design §9, §17). Implements {@link EgressMetrics} so
 * {@code libs/nats-egress} destinations can report through this same bean, exactly as phase-1's
 * {@code SequencerMetrics} does.
 *
 * <p>{@code bridge_gap_total} should be permanently zero: the design's phase-2 requirement is
 * that egress is contiguous by construction (no permitted gaps, unlike phase-1). A nonzero
 * reading here means the cluster's egress itself skipped a sequenceId — a blocking find during
 * testing, never routine noise (design §9).
 */
public class BridgeMetrics implements EgressMetrics {

    private final LongAdder gapTotal = new LongAdder();
    private final LongAdder publishFailuresTotal = new LongAdder();
    private final LongAdder droppedTotal = new LongAdder();
    private final LongAdder backpressureStallNanosTotal = new LongAdder();
    private final LongAdder loopErrorsTotal = new LongAdder();
    private final LongAdder checkpointResetTotal = new LongAdder();

    private volatile long messagesBridgedTotal;
    private volatile long lastBridgedSequenceId;
    private volatile long batchesTotal;
    private volatile int inflightWindow;
    private volatile boolean replayCatchUpActive;

    public BridgeMetrics(MeterRegistry registry) {
        registry.gauge("bridge_gap_total", gapTotal, LongAdder::sum);
        registry.gauge("bridge_messages_total", this, m -> m.messagesBridgedTotal);
        registry.gauge("bridge_last_sequence_id", this, m -> m.lastBridgedSequenceId);
        registry.gauge("bridge_batches_total", this, m -> m.batchesTotal);
        registry.gauge("bridge_inflight_window", this, m -> m.inflightWindow);
        registry.gauge("bridge_publish_failures_total", publishFailuresTotal, LongAdder::sum);
        registry.gauge("bridge_dropped_total", droppedTotal, LongAdder::sum);
        registry.gauge("bridge_backpressure_stall_seconds_total", backpressureStallNanosTotal,
                adder -> adder.sum() / 1_000_000_000.0);
        // Design §17: a precise byte/sequence-accurate lag isn't cleanly computable from the
        // bridge side alone (ReplayMerge exposes no public "how far behind" accessor, and there's
        // no leader-side high-water-mark broadcast to compare against — see BridgePipeline's
        // Javadoc). This is a coarse but real and useful proxy instead: every message the bridge
        // does receive is republished immediately (no internal queueing), so the only time it is
        // genuinely behind the live stream is while ReplayMerge is still catching up from the
        // Archive after downtime. 1 while that catch-up is in progress, 0 once merged/live.
        registry.gauge("bridge_lag_sequences", this, m -> m.replayCatchUpActive ? 1 : 0);
        registry.gauge("bridge_loop_errors_total", loopErrorsTotal, LongAdder::sum);
        // Distinct from bridge_gap_total on purpose (see ContiguityTracker's class Javadoc): a
        // gap is an egress-side bug, a checkpoint reset is this bridge recovering from a stale
        // local checkpoint (e.g. cluster-node's sequenceId space restarting after losing
        // non-persisted state) - conflating the two would hide a real distinction operators need.
        // Should be permanently zero against a genuinely production-safe multi-member cluster
        // (design §9); nonzero locally is expected whenever cluster-node loses state.
        registry.gauge("bridge_checkpoint_reset_total", checkpointResetTotal, LongAdder::sum);
    }

    /** Records one successfully bridged (republished) message. */
    public void onMessageBridged(long sequenceId) {
        messagesBridgedTotal++;
        lastBridgedSequenceId = sequenceId;
    }

    /**
     * Records a contiguity break in consumed sequenceIds — should never happen (design §9).
     *
     * @param gapSize number of missing sequenceIds between the last bridged one and this one
     */
    public void onGapDetected(long gapSize) {
        gapTotal.add(gapSize);
    }

    @Override
    public void onBatchFlushed(int messageCount) {
        batchesTotal++;
    }

    @Override
    public void setInflightWindow(int inflight) {
        this.inflightWindow = inflight;
    }

    @Override
    public void incrementPublishFailures() {
        publishFailuresTotal.increment();
    }

    @Override
    public void incrementDropped() {
        droppedTotal.increment();
    }

    @Override
    public void recordBackpressureStall(long nanos) {
        backpressureStallNanosTotal.add(nanos);
    }

    /** See {@code bridge_lag_sequences}'s registration comment for what this coarse signal means. */
    public void setReplayCatchUpActive(boolean active) {
        this.replayCatchUpActive = active;
    }

    /**
     * One iteration of the bridge poll loop threw instead of completing normally (e.g. a
     * checkpoint write failing) — should be permanently zero on a healthy destination; nonzero
     * means the bridge is surviving a transient failure rather than dying silently, which is the
     * point, but still worth alerting on.
     */
    public void onLoopError() {
        loopErrorsTotal.increment();
    }

    /**
     * Records the contiguity tracker rebasing a stale checkpoint after a sustained run of
     * below-checkpoint sequenceIds (see {@code ContiguityTracker}'s class Javadoc) — an event
     * count, not a magnitude like {@link #onGapDetected}.
     */
    public void onCheckpointReset() {
        checkpointResetTotal.increment();
    }
}
