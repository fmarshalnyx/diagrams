package gcm.md.linehandlertemplate.relay;

import gcm.md.linehandlertemplate.config.LineHandlerProperties;
import gcm.md.linehandlertemplate.metrics.LineHandlerMetrics;
import gcm.md.sequencer.clusterclient.ClusterIngressClient;
import gcm.md.sequencer.ingress.IngressTransport;
import io.nats.client.JetStream;
import io.nats.client.JetStreamSubscription;
import io.nats.client.Message;
import io.nats.client.PullSubscribeOptions;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.springframework.context.SmartLifecycle;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BooleanSupplier;

/**
 * Fetches from the upstream JetStream durable consumer, stamps each message's {@code
 * sourceSeqNum}, and relays it via whichever {@code IngressTransport} is configured — the whole
 * pattern a real line handler should copy (see this module's README for exactly which parts).
 *
 * <p><b>The crash-recovery contract, and why there's no handler-side bookkeeping anywhere in this
 * class:</b> a message is only {@link Message#ack() ack}ed <i>after</i> {@link #offerWithRetry}
 * returns successfully. If this process dies mid-retry, the unacked message (and everything
 * after it — JetStream's durable-consumer redelivery is sequential) is simply redelivered on
 * next connect, with the identical {@link io.nats.client.impl.NatsJetStreamMetaData#streamSequence()}.
 * Since that stream sequence number <i>is</i> the {@code sourceSeqNum} this class stamps (see
 * {@link #onFetched}), the redelivered message gets the identical {@code sourceSeqNum} on
 * re-offer, which the sequencer's own dedup (per {@code libs/cluster-client}'s package-info
 * idempotency contract) recognizes as an already-seen duplicate and safely no-ops. No local
 * file, database, or counter is needed anywhere in this relay — durability lives entirely in
 * JetStream's own consumer state, which already survives a restart by design.
 */
public final class UpstreamRelay implements SmartLifecycle {

    private final JetStream upstreamJetStream;
    private final LineHandlerProperties.Upstream upstreamConfig;
    private final IngressTransport transport;
    private final SourceSeqNumStamper stamper;
    private final LineHandlerMetrics metrics;
    private final long retryParkNanos;
    private final int warnEveryNAttempts;

    private volatile boolean running;
    private Thread relayThread;
    private long lastStreamSequenceSeen;

    public UpstreamRelay(JetStream upstreamJetStream, LineHandlerProperties properties,
                          IngressTransport transport, SourceSeqNumStamper stamper, LineHandlerMetrics metrics) {
        this.upstreamJetStream = upstreamJetStream;
        this.upstreamConfig = properties.getUpstream();
        this.transport = transport;
        this.stamper = stamper;
        this.metrics = metrics;
        this.retryParkNanos = properties.getOffer().getRetryParkNanos();
        this.warnEveryNAttempts = properties.getOffer().getWarnEveryNAttempts();
    }

    @Override
    public void start() {
        running = true;
        relayThread = new Thread(this::run, "line-handler-relay");
        relayThread.setDaemon(true);
        relayThread.start();
    }

    @Override
    public void stop() {
        running = false;
        if (relayThread != null) {
            relayThread.interrupt();
        }
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    /** Test-only: lets tests exercise {@link #onFetched} without a real {@link #start()}'d background thread. */
    void markRunningForTest() {
        this.running = true;
    }

    private void run() {
        JetStreamSubscription subscription = subscribe();
        Duration fetchWait = Duration.ofMillis(upstreamConfig.getFetchWaitMillis());
        while (running) {
            try {
                List<Message> messages = subscription.fetch(upstreamConfig.getMaxFetchBatch(), fetchWait);
                if (messages.isEmpty()) {
                    // Nothing to relay right now — still poll the Aeron transport's egress so its
                    // cluster session's keep-alive doesn't lapse during idle stretches. offer()
                    // calls this internally too, but only while actively offering; this covers
                    // the gap between bursts. IngressTransport itself has no pollEgress() (NATS
                    // has no equivalent concept), so this is a deliberate, documented type-check
                    // rather than a general pattern to replicate elsewhere in this class.
                    if (transport instanceof ClusterIngressClient clusterIngressClient) {
                        clusterIngressClient.pollEgress();
                    }
                    continue;
                }
                for (Message message : messages) {
                    if (!running) {
                        return;
                    }
                    onFetched(message);
                }
            } catch (RuntimeException transientFailure) {
                // A single iteration failing (e.g. ClusterIngressClient's reconnect hitting a
                // cluster-connect timeout) must not kill this thread: it's a daemon thread with
                // no supervisor, so an uncaught exception here would silently and permanently
                // stop relaying while isRunning()/the readiness probe both keep reporting healthy
                // — exactly the kind of "crash" this class's whole no-local-bookkeeping design
                // (see class Javadoc) is supposed to make impossible. Nothing was acked, so
                // JetStream redelivers on the next successful fetch; just log, count, and retry.
                metrics.onRelayLoopError();
                System.err.println("line-handler-template: relay iteration failed, retrying: "
                        + transientFailure);
                LockSupport.parkNanos(retryParkNanos);
            }
        }
    }

    private JetStreamSubscription subscribe() {
        try {
            PullSubscribeOptions options = PullSubscribeOptions.builder()
                    .stream(upstreamConfig.getStream())
                    .durable(upstreamConfig.getDurableConsumerName())
                    .build();
            return upstreamJetStream.subscribe(upstreamConfig.getSubject(), options);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to subscribe to upstream subject "
                    + upstreamConfig.getSubject(), e);
        }
    }

    /** Package-private and Aeron/NATS-transport-agnostic so it's directly unit-testable. */
    void onFetched(Message message) {
        long streamSequence = message.metaData().streamSequence();
        if (streamSequence <= lastStreamSequenceSeen) {
            metrics.onUpstreamRedelivery();
        }
        lastStreamSequenceSeen = Math.max(lastStreamSequenceSeen, streamSequence);

        UnsafeBuffer buffer = new UnsafeBuffer(message.getData());
        stamper.stamp(buffer, streamSequence);

        if (offerWithRetry(buffer, 0, buffer.capacity())) {
            message.ack();
            metrics.onMessageRelayed(streamSequence);
        }
        // else: stop() was called mid-retry — return without acking, per this class's Javadoc.
    }

    /** Delegates to {@link #offerWithRetry(DirectBuffer, int, int, BooleanSupplier)} bound to this instance's {@code running} flag. */
    boolean offerWithRetry(DirectBuffer buffer, int offset, int length) {
        return offerWithRetry(buffer, offset, length, () -> running);
    }

    /**
     * Retries {@link IngressTransport#offer} until it succeeds or {@code keepRunning} turns
     * false — bounded by responsiveness to that condition, not by attempt count, since {@code
     * offer}'s own contract already bounds each individual attempt's blocking (see {@code
     * ClusterIngressClient}'s and {@code NatsIngressTransport}'s own Javadoc). Takes {@code
     * keepRunning} as a parameter, rather than reading {@link #running} directly, purely so tests
     * can drive the "shutdown mid-retry" path deterministically without needing a real background
     * thread.
     *
     * @return {@code true} once offered successfully; {@code false} only if {@code keepRunning}
     *         turned false before that happened.
     */
    boolean offerWithRetry(DirectBuffer buffer, int offset, int length, BooleanSupplier keepRunning) {
        int attempts = 0;
        while (keepRunning.getAsBoolean()) {
            long result = transport.offer(buffer, offset, length);
            if (result >= 0) {
                return true;
            }
            attempts++;
            metrics.onOfferRetry();
            if (attempts % warnEveryNAttempts == 0) {
                System.err.println("line-handler-template: offer backpressured for " + attempts
                        + " attempts");
            }
            LockSupport.parkNanos(retryParkNanos);
        }
        return false;
    }
}
