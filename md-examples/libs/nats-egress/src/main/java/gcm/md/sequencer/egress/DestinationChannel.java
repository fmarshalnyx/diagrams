package gcm.md.sequencer.egress;

import org.agrona.DirectBuffer;

/**
 * Pluggable sequencer egress (project spec §7). Implementations are selected purely by
 * {@code sequencer.egress.type} config; {@link gcm.md.sequencer.egress.BatchingDestination}
 * optionally wraps either one as a decorator.
 */
public interface DestinationChannel {

    /** Hot path: publishes one stamped message. Must not block under normal operation. */
    void publish(DirectBuffer buffer, int offset, int length);

    /** Forces any buffered/batched state out immediately (batcher linger, idle-stream flush). */
    void flush();

    /** Drains in-flight work and closes. Idempotent. */
    void stop();

    /**
     * Blocks until every publish accepted so far by this channel has been durably confirmed
     * (JetStream: publish ack received; core NATS: already true once {@link #publish} returns).
     * Used by ingress to implement ack-after-publish-confirmed semantics (spec §5) without
     * widening the destination contract beyond what most callers need.
     */
    default void awaitInFlightDrained() {
        // No-op by default: only destinations with an async in-flight ack window need this.
    }
}
