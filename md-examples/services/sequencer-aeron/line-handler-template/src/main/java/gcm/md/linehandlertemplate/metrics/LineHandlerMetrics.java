package gcm.md.linehandlertemplate.metrics;

import gcm.md.sequencer.egress.EgressMetrics;
import io.micrometer.core.instrument.MeterRegistry;

import java.util.concurrent.atomic.LongAdder;

/**
 * Line-handler-template observability. Implements {@link EgressMetrics} because
 * {@code NatsIngressTransport}'s constructor requires one (satisfied even when
 * {@code ingressTransport=aeron}, in which case these particular counters are simply never
 * incremented by that transport — {@code ClusterIngressClient} doesn't use this interface at
 * all). Also exposes handler-specific gauges independent of which transport is selected.
 */
public class LineHandlerMetrics implements EgressMetrics {

    private final LongAdder publishFailuresTotal = new LongAdder();
    private final LongAdder droppedTotal = new LongAdder();
    private final LongAdder backpressureStallNanosTotal = new LongAdder();
    private final LongAdder messagesRelayedTotal = new LongAdder();
    private final LongAdder offerRetriesTotal = new LongAdder();
    private final LongAdder upstreamRedeliveriesTotal = new LongAdder();
    private final LongAdder relayLoopErrorsTotal = new LongAdder();

    private volatile long lastSourceSeqNum;
    private volatile int inflightWindow;

    public LineHandlerMetrics(MeterRegistry registry) {
        registry.gauge("line_handler_messages_relayed_total", messagesRelayedTotal, LongAdder::sum);
        registry.gauge("line_handler_last_source_seq_num", this, m -> m.lastSourceSeqNum);
        registry.gauge("line_handler_offer_retries_total", offerRetriesTotal, LongAdder::sum);
        registry.gauge("line_handler_upstream_redeliveries_total", upstreamRedeliveriesTotal, LongAdder::sum);
        registry.gauge("line_handler_inflight_window", this, m -> m.inflightWindow);
        registry.gauge("line_handler_publish_failures_total", publishFailuresTotal, LongAdder::sum);
        registry.gauge("line_handler_backpressure_stall_seconds_total", backpressureStallNanosTotal,
                adder -> adder.sum() / 1_000_000_000.0);
        registry.gauge("line_handler_relay_loop_errors_total", relayLoopErrorsTotal, LongAdder::sum);
    }

    /** Records one message successfully offered and acked upstream. */
    public void onMessageRelayed(long sourceSeqNum) {
        messagesRelayedTotal.increment();
        lastSourceSeqNum = sourceSeqNum;
    }

    /** One retry attempt on a negative {@code offer()} result. */
    public void onOfferRetry() {
        offerRetriesTotal.increment();
    }

    /**
     * A fetched message's {@code streamSequence()} was {@code <=} the last one seen before a
     * restart — purely informational (confirms the crash-recovery redelivery path actually fired
     * during this run), never consulted for dedup logic, which belongs entirely to the sequencer.
     */
    public void onUpstreamRedelivery() {
        upstreamRedeliveriesTotal.increment();
    }

    /**
     * One iteration of the relay loop (a transport offer/reconnect, most commonly) threw instead
     * of returning a backpressure result — should be permanently zero on a healthy transport;
     * nonzero means the relay is surviving transient failures rather than dying silently, which
     * is the point, but still worth alerting on.
     */
    public void onRelayLoopError() {
        relayLoopErrorsTotal.increment();
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

    @Override
    public void onBatchFlushed(int messageCount) {
        // Not applicable to ingress — no-op, satisfies the interface.
    }
}
