package gcm.md.sequencer.stamping;

/**
 * Reports engine events through callbacks so {@link StampingEngine} never touches a metrics
 * library (design §4 "Listener/metrics indirection" — the same pattern
 * {@code libs/nats-egress}'s {@code EgressMetrics} applies to egress). Hosts map these onto
 * whatever metrics bean they run; all methods default to a no-op so a test or a host that only
 * cares about some events can implement just those.
 *
 * <p>Called from the single engine thread only — implementations must be allocation-free and
 * must not block.
 */
public interface EngineListener {

    /** A message failed the schemaId sanity guard and was rejected without being stamped. */
    default void onSchemaMismatch() {
    }

    /** A MatchEventBoundary's eventId could not be tracked because {@code maxTrackedEvents} was reached. */
    default void onEventTrackingEvicted() {
    }

    /** A source republished an already-seen {@code sourceSeqNum}; the message was skipped. */
    default void onSourceDuplicate(long sourceId) {
    }

    /** A source's {@code sourceSeqNum} jumped by more than one; {@code gapSize} messages were lost upstream. */
    default void onSourceSeqGap(long sourceId, long gapSize) {
    }
}
