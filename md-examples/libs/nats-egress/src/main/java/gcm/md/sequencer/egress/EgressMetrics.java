package gcm.md.sequencer.egress;

/**
 * Egress-side counters/gauges, reported through a listener interface so this module never
 * depends on a metrics library (project spec §4 "Listener/metrics indirection" pattern, applied
 * to egress). Hosts implement this against whatever metrics bean they already run.
 */
public interface EgressMetrics {

    /** Increments the dropped-message counter under the {@code drop} backpressure policy. */
    void incrementDropped();

    /** Increments the publish-failure counter; safe to call from an async ack-callback thread. */
    void incrementPublishFailures();

    /** Records a completed backpressure stall of the given duration, in nanoseconds. */
    void recordBackpressureStall(long nanos);

    /** Updates the current async in-flight publish window occupancy. */
    void setInflightWindow(int inflight);

    /** Records a flushed {@code MessageBatch}'s message count. */
    void onBatchFlushed(int messageCount);
}
