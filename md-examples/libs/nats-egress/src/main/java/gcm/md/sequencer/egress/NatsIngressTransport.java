package gcm.md.sequencer.egress;

import gcm.md.sequencer.ingress.IngressTransport;
import io.nats.client.JetStream;
import org.agrona.DirectBuffer;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * NATS JetStream {@link IngressTransport} — the config-selectable alternative to
 * {@code gcm.md.sequencer.clusterclient.ClusterIngressClient} (see {@code libs/ingress-transport}'s
 * package-info for why line handlers pick a transport by config rather than migrating through a
 * shim service). Publishes to the same subject phase-1's sequencer already consumes; the
 * sequencer-side dedupe on {@code sourceSeqNum} makes redelivery-after-crash safe no matter which
 * transport a line handler is configured with.
 *
 * <p>Uses the same bounded in-flight async-ack window as {@link JetStreamDestination}, but,
 * per {@link IngressTransport#offer}'s contract, never blocks or silently drops on backpressure —
 * a full window is surfaced to the caller as a negative return, exactly like
 * {@code ClusterIngressClient}'s bounded-retry-then-surface behavior, so a line handler's feed
 * thread can spill or slow its upstream instead of stalling.
 */
public final class NatsIngressTransport implements IngressTransport {

    private static final long BACK_PRESSURED = -1L;

    private final JetStream jetStream;
    private final String subject;
    private final int maxInFlight;
    private final EgressMetrics metrics;
    private final AtomicInteger inflight = new AtomicInteger();

    /** Creates the transport against an already-connected {@link JetStream} context. */
    public NatsIngressTransport(JetStream jetStream, NatsIngressConfig config, EgressMetrics metrics) {
        this.jetStream = jetStream;
        this.subject = config.natsSubject();
        this.maxInFlight = config.maxInFlight();
        this.metrics = metrics;
    }

    @Override
    public long offer(DirectBuffer buffer, int offset, int length) {
        if (inflight.get() >= maxInFlight) {
            return BACK_PRESSURED;
        }
        byte[] bytes = NatsBufferUtil.toByteArray(buffer, offset, length);
        int position = inflight.incrementAndGet();
        metrics.setInflightWindow(position);
        jetStream.publishAsync(subject, bytes).whenComplete((ack, failure) -> {
            metrics.setInflightWindow(inflight.decrementAndGet());
            if (failure != null) {
                metrics.incrementPublishFailures();
            }
        });
        return position;
    }
}
