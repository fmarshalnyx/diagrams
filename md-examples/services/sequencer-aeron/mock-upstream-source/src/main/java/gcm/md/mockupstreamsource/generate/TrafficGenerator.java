package gcm.md.mockupstreamsource.generate;

import gcm.md.mockupstreamsource.config.MockUpstreamSourceProperties;
import gcm.md.mockupstreamsource.metrics.MockUpstreamSourceMetrics;
import io.nats.client.Connection;
import io.nats.client.JetStream;
import io.nats.client.JetStreamManagement;
import io.nats.client.api.RetentionPolicy;
import io.nats.client.api.StorageType;
import io.nats.client.api.StreamConfiguration;
import org.springframework.context.SmartLifecycle;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * Continuously generates synthetic upstream traffic — the persistent-service replacement for
 * {@code sequencer-loadgen}'s duration-bound publish loop. Starts publishing immediately on
 * {@link #start()} and runs until {@link #stop()}, exactly like every other continuous pipeline
 * in this reactor ({@code UpstreamRelay}, {@code BridgePipeline}) — there is no
 * {@code --duration-seconds} concept anymore; scale the Deployment to 0 replicas to turn traffic
 * off rather than relying on an in-process timer.
 */
public final class TrafficGenerator implements SmartLifecycle {

    private static final long BURST_WINDOW_NANOS = TimeUnit.MILLISECONDS.toNanos(100);
    private static final long QUIET_WINDOW_NANOS = TimeUnit.MILLISECONDS.toNanos(400);

    private final Connection connection;
    private final String stream;
    private final String subject;
    private final long ratePerSecond;
    private final TrafficPattern pattern;
    private final MessagePlanner planner;
    private final MessageEncoder encoder;
    private final MockUpstreamSourceMetrics metrics;

    private volatile boolean running;
    private Thread generatorThread;

    public TrafficGenerator(Connection connection, MockUpstreamSourceProperties properties,
                             MessagePlanner planner, MessageEncoder encoder, MockUpstreamSourceMetrics metrics) {
        this.connection = connection;
        this.stream = properties.getStream();
        this.subject = properties.getSubject();
        this.ratePerSecond = properties.getRate();
        this.pattern = TrafficPattern.parse(properties.getPattern());
        this.planner = planner;
        this.encoder = encoder;
        this.metrics = metrics;
    }

    @Override
    public void start() {
        running = true;
        generatorThread = new Thread(this::run, "mock-upstream-generator");
        generatorThread.setDaemon(true);
        generatorThread.start();
    }

    @Override
    public void stop() {
        running = false;
        if (generatorThread != null) {
            generatorThread.interrupt();
        }
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    private void run() {
        try {
            ensureStreamExists();
        } catch (Exception e) {
            throw new IllegalStateException("Failed to ensure upstream stream '" + stream + "' exists", e);
        }
        JetStream jetStream;
        try {
            jetStream = connection.jetStream();
        } catch (Exception e) {
            throw new IllegalStateException("Failed to obtain JetStream context", e);
        }

        if (pattern == TrafficPattern.BURSTY) {
            runBursty(jetStream);
        } else {
            runSteady(jetStream);
        }
    }

    /**
     * Idempotent: safe to call on every start, including restarts against an already-created
     * stream. WorkQueue retention (messages removed once the sole durable consumer — {@code
     * line-handler-template}'s {@code UpstreamRelay} — acks them) plus a generous {@code maxAge}
     * safety net are not optional here: unlike the one-shot {@code sequencer-loadgen} CLI this
     * replaced, this service runs forever, so an unbounded (default-retention) stream grows
     * without limit. A real incident, not a hypothetical: this stream hit the NATS server's
     * {@code max_file_store} quota (1Gi) during this session, at which point *every* JetStream
     * write on the server — including {@code nats-bridge}'s unrelated republish to {@code
     * MD_SEQUENCED} — started failing with "insufficient resources", which surfaced as both
     * {@code bridge_publish_failures} and {@code mock_upstream_gap} on the observed egress. The
     * {@code maxAge} bound also protects against the exact scenario that caused that incident:
     * the consumer being down/broken for an extended period while this keeps publishing.
     *
     * <p>{@code maxBytes} is a second, harder line of defense on top of WorkQueue retention, added
     * after WorkQueue alone still wasn't enough: WorkQueue only removes <em>acked</em> messages,
     * so if {@link #ratePerSecond} (a stress-test knob, can be set well above what the rest of the
     * local pipeline can actually drain) sustainably outpaces the sole consumer, the backlog grows
     * without bound regardless of retention policy — confirmed live in this same incident, where
     * consumption briefly stalling (an unrelated `line-handler-template` bug, since fixed) let the
     * stream reach 5.6M unacked messages before quota exhaustion. {@code maxBytes} caps the
     * backlog itself (oldest unacked messages are dropped once exceeded) so a sustained rate
     * mismatch degrades to bounded message loss instead of taking down the whole NATS server.
     */
    private void ensureStreamExists() throws Exception {
        JetStreamManagement jsm = connection.jetStreamManagement();
        try {
            jsm.addStream(StreamConfiguration.builder()
                    .name(stream)
                    .subjects(subject)
                    .storageType(StorageType.File)
                    .retentionPolicy(RetentionPolicy.WorkQueue)
                    .maxAge(Duration.ofHours(1))
                    .maxBytes(256L * 1024 * 1024)
                    .build());
        } catch (Exception addStreamFailure) {
            // Usually just "stream already exists" from a prior run — expected on every restart
            // but the first (same idempotent-create pattern as nats-bridge's checkpoint KV
            // bucket). But this catch was previously silent, which once hid a real failure here
            // (a transient race at startup) behind confusing "503 No Responders" publish errors
            // with no clue why — log it so a genuine failure is at least visible next time.
            System.err.println("mock-upstream-source: addStream for '" + stream
                    + "' failed (expected if it already exists): " + addStreamFailure.getMessage());
        }
    }

    private void runSteady(JetStream jetStream) {
        long intervalNanos = intervalNanos();
        long nextSendAt = System.nanoTime();
        while (running) {
            long now = System.nanoTime();
            if (now < nextSendAt) {
                continue; // busy-spin to the next send slot, matching loadgen's pacing approach
            }
            emitOne(jetStream);
            nextSendAt += intervalNanos;
        }
    }

    /** Peak rate applies only during each burst window; the rest of the duty cycle is quiet. */
    private void runBursty(JetStream jetStream) {
        long intervalNanos = intervalNanos();
        while (running) {
            long burstEndAt = System.nanoTime() + BURST_WINDOW_NANOS;
            long nextSendAt = System.nanoTime();
            while (running && System.nanoTime() < burstEndAt) {
                long now = System.nanoTime();
                if (now < nextSendAt) {
                    continue;
                }
                emitOne(jetStream);
                nextSendAt += intervalNanos;
            }
            long quietEndAt = System.nanoTime() + QUIET_WINDOW_NANOS;
            while (running && System.nanoTime() < quietEndAt) {
                Thread.onSpinWait();
            }
        }
    }

    private long intervalNanos() {
        return Math.max(1, TimeUnit.SECONDS.toNanos(1) / Math.max(1, ratePerSecond));
    }

    /** Package-private and JetStream-mockable so it's directly unit-testable. */
    void emitOne(JetStream jetStream) {
        MessagePlanner.PlannedMessage planned = planner.next();
        if (planned.decision() == MessagePlanner.Decision.SKIP) {
            return;
        }
        byte[] message = encoder.encode(planned.sourceSeqNum());
        if (!publish(jetStream, message)) {
            return;
        }
        if (planned.decision() == MessagePlanner.Decision.PUBLISH_THEN_DUPLICATE) {
            publish(jetStream, message);
        }
    }

    private boolean publish(JetStream jetStream, byte[] message) {
        try {
            jetStream.publish(subject, message);
            metrics.onPublished();
            return true;
        } catch (Exception e) {
            System.err.println("mock-upstream-source: publish failed: " + e.getMessage());
            return false;
        }
    }
}
