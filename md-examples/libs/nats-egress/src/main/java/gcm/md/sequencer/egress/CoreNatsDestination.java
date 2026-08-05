package gcm.md.sequencer.egress;

import io.nats.client.Connection;
import org.agrona.DirectBuffer;

import java.util.concurrent.locks.LockSupport;

/**
 * Fire-and-forget core NATS egress (project spec §7). Order is preserved on the single
 * connection; loss is possible but immediately detectable downstream via sequenceId gaps —
 * recovery is an out-of-band replay service's job. No persistence, no acks: easily exceeds
 * 1M msgs/sec.
 */
public final class CoreNatsDestination implements DestinationChannel {

    private static final long SPIN_THEN_PARK_NANOS = 50_000; // ~50us of spin before parking

    private final Connection connection;
    private final String subject;
    private final boolean blockOnBackpressure;
    private final long maxStallNanos;
    private final EgressMetrics metrics;

    /** Creates the destination against an already-connected {@link Connection}. */
    public CoreNatsDestination(Connection connection, EgressConfig config, EgressMetrics metrics) {
        this.connection = connection;
        this.subject = config.natsSubject();
        this.blockOnBackpressure = config.blockOnBackpressure();
        this.maxStallNanos = config.maxStallMs() * 1_000_000L;
        this.metrics = metrics;
    }

    @Override
    public void publish(DirectBuffer buffer, int offset, int length) {
        byte[] bytes = toByteArray(buffer, offset, length);
        long stallStart = 0L;
        while (true) {
            try {
                connection.publish(subject, bytes);
                if (stallStart != 0L) {
                    metrics.recordBackpressureStall(System.nanoTime() - stallStart);
                }
                return;
            } catch (IllegalStateException fullOutgoingQueue) {
                if (!blockOnBackpressure) {
                    metrics.incrementDropped();
                    return;
                }
                if (stallStart == 0L) {
                    stallStart = System.nanoTime();
                }
                if (System.nanoTime() - stallStart > maxStallNanos) {
                    // Alarm condition (sequencer_backpressure_stall_seconds past max-stall-ms);
                    // keep retrying — market-data consumers prefer a stall over silent loss.
                }
                LockSupport.parkNanos(SPIN_THEN_PARK_NANOS);
            }
        }
    }

    @Override
    public void flush() {
        // Core NATS already coalesces at the TCP level; nothing buffered at this layer.
    }

    @Override
    public void stop() {
        // Connection lifecycle (drain/close) is owned centrally; nothing destination-local to drain.
    }

    /**
     * jnats' core publish() takes a bare {@code byte[]} with no offset/length, so a full-array
     * match (the common case: this buffer wraps the byte[] the NATS client already allocated
     * per inbound message) is passed straight through allocation-free; any other case must copy.
     */
    private byte[] toByteArray(DirectBuffer buffer, int offset, int length) {
        byte[] backing = buffer.byteArray();
        if (backing != null && offset == 0 && length == backing.length) {
            return backing;
        }
        byte[] exact = new byte[length];
        buffer.getBytes(offset, exact, 0, length);
        return exact;
    }
}
