package gcm.md.sequencer.egress;

import io.nats.client.JetStream;
import org.agrona.DirectBuffer;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

/**
 * Async JetStream egress with a bounded in-flight publish-ack window (project spec §7).
 * Ack futures are reaped off the hot thread: completion callbacks (which run on a jnats
 * internal thread) decrement the in-flight counter and bump failure metrics, never touching
 * the stamping thread. Publish order on the single connection preserves stream order.
 *
 * <p>Realistic single-publisher ceiling without batching is ~200-600k msgs/sec; batching
 * (see {@link BatchingDestination}) is required to reach 1M logical msgs/sec (spec §7/§8).
 */
public final class JetStreamDestination implements DestinationChannel {

    private static final long SPIN_THEN_PARK_NANOS = 50_000; // ~50us of spin before parking

    private final JetStream jetStream;
    private final String subject;
    private final int maxInFlight;
    private final boolean blockOnBackpressure;
    private final long maxStallNanos;
    private final EgressMetrics metrics;

    private final AtomicInteger inflight = new AtomicInteger();

    /** Creates the destination against an already-connected {@link JetStream} context. */
    public JetStreamDestination(JetStream jetStream, EgressConfig config, EgressMetrics metrics) {
        this.jetStream = jetStream;
        this.subject = config.natsSubject();
        this.maxInFlight = config.jetStreamMaxInFlight();
        this.blockOnBackpressure = config.blockOnBackpressure();
        this.maxStallNanos = config.maxStallMs() * 1_000_000L;
        this.metrics = metrics;
    }

    @Override
    public void publish(DirectBuffer buffer, int offset, int length) {
        if (!awaitWindowRoom()) {
            metrics.incrementDropped();
            return;
        }
        byte[] bytes = toByteArray(buffer, offset, length);
        inflight.incrementAndGet();
        metrics.setInflightWindow(inflight.get());
        jetStream.publishAsync(subject, bytes).whenComplete((ack, failure) -> {
            metrics.setInflightWindow(inflight.decrementAndGet());
            if (failure != null) {
                metrics.incrementPublishFailures();
            }
        });
    }

    /** @return {@code true} if there is now room in the window; {@code false} means "drop this message." */
    private boolean awaitWindowRoom() {
        long stallStart = 0L;
        while (inflight.get() >= maxInFlight) {
            if (!blockOnBackpressure) {
                return false;
            }
            if (stallStart == 0L) {
                stallStart = System.nanoTime();
            }
            if (System.nanoTime() - stallStart > maxStallNanos) {
                // Alarm condition (sequencer_backpressure_stall_seconds past max-stall-ms);
                // keep waiting — market-data consumers prefer a stall over silent loss.
            }
            LockSupport.parkNanos(SPIN_THEN_PARK_NANOS);
        }
        if (stallStart != 0L) {
            metrics.recordBackpressureStall(System.nanoTime() - stallStart);
        }
        return true;
    }

    @Override
    public void flush() {
        // publishAsync already dispatches immediately; nothing buffered at this layer.
    }

    @Override
    public void awaitInFlightDrained() {
        while (inflight.get() > 0) {
            LockSupport.parkNanos(SPIN_THEN_PARK_NANOS);
        }
    }

    @Override
    public void stop() {
        awaitInFlightDrained();
    }

    private byte[] toByteArray(DirectBuffer buffer, int offset, int length) {
        return NatsBufferUtil.toByteArray(buffer, offset, length);
    }
}
