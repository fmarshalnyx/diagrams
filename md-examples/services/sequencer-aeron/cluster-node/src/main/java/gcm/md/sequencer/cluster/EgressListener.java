package gcm.md.sequencer.cluster;

/**
 * Reports {@link AeronEgressPublisher} events through callbacks so it never touches a metrics
 * library directly (same "listener indirection" pattern as
 * {@link gcm.md.sequencer.stamping.EngineListener} / {@code libs/nats-egress}'s
 * {@code EgressMetrics}). All methods default to a no-op. Called only from the clustered
 * service's single thread — implementations must be allocation-free and must not block.
 */
public interface EgressListener {

    /** A message was dropped by the suppression gate (design §6.4: {@code sequencer_egress_suppressed_total}). */
    default void onSuppressed(long sequenceId) {
    }

    /** A completed backpressure stall of the given duration, in nanoseconds (design §6.4, phase-1 §9 {@code block} mode). */
    default void onBackpressureStall(long nanos) {
    }
}
