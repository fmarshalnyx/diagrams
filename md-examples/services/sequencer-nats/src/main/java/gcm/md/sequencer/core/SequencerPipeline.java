package gcm.md.sequencer.core;

import gcm.md.sequencer.config.SequencerProperties;
import gcm.md.sequencer.egress.DestinationChannel;
import gcm.md.sequencer.heartbeat.HeartbeatEmitter;
import gcm.md.sequencer.ingress.IngressChannel;
import gcm.md.sequencer.metrics.SequencerMetrics;
import io.nats.client.Connection;
import io.nats.client.JetStreamApiException;
import io.nats.client.KeyValueManagement;
import io.nats.client.api.KeyValueConfiguration;
import io.nats.client.api.StorageType;
import org.agrona.concurrent.UnsafeBuffer;
import org.springframework.context.SmartLifecycle;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Owns the strict startup/failover ordering and single-writer enforcement described in the
 * project spec §10: (1) connect + verify egress and KV bucket, (2) acquire leadership,
 * (3) lease a sequence block, (4) subscribe ingress. Readiness is all four.
 *
 * <p>The stamp-allocate-publish critical section ({@link #stampAndPublish}) is guarded by a
 * lock. This is a deliberate, documented deviation from the spec's "plain long, no atomics"
 * ideal: it exists solely to let the periodic sequencer heartbeat (§8, driven by its own timer,
 * independent of ingress traffic — otherwise a truly idle core-mode ingress would never emit
 * one) interleave safely with the ingress hot thread. The lock is uncontended in the
 * overwhelming common case (heartbeats fire every 100ms by default, not per-message), so its
 * cost is negligible against the 1us/message budget.
 */
public final class SequencerPipeline implements SmartLifecycle {

    private final IngressChannel ingress;
    private final DestinationChannel destination;
    private final SequenceStamper stamper;
    private final SequenceAllocator allocator;
    private final LeaderElection leaderElection;
    private final HeartbeatEmitter heartbeat;
    private final SequencerMetrics metrics;
    private final KeyValueManagement kvManagement;
    private final Connection ingressConnection;
    private final Connection egressConnection;
    private final String kvBucket;

    private final Object hotPathLock = new Object();
    private final UnsafeBuffer buffer = new UnsafeBuffer(new byte[0]);

    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicBoolean ingressSubscribed = new AtomicBoolean(false);
    private final AtomicBoolean readinessFailed = new AtomicBoolean(false);
    private volatile boolean firstMessageProcessed = false;

    private Thread startupThread;
    private ScheduledExecutorService heartbeatScheduler;

    /** Wires every collaborator needed to drive the pipeline; see class javadoc for ordering. */
    public SequencerPipeline(IngressChannel ingress, DestinationChannel destination, SequenceStamper stamper,
                              SequenceAllocator allocator, LeaderElection leaderElection, HeartbeatEmitter heartbeat,
                              SequencerMetrics metrics, KeyValueManagement kvManagement, Connection ingressConnection,
                              Connection egressConnection, SequencerProperties properties) {
        this.ingress = ingress;
        this.destination = destination;
        this.stamper = stamper;
        this.allocator = allocator;
        this.leaderElection = leaderElection;
        this.heartbeat = heartbeat;
        this.metrics = metrics;
        this.kvManagement = kvManagement;
        this.ingressConnection = ingressConnection;
        this.egressConnection = egressConnection;
        this.kvBucket = properties.getAllocator().getKvBucket();
    }

    @Override
    public void start() {
        if (!running.compareAndSet(false, true)) {
            return;
        }
        // Runs on a background thread: leadership acquisition blocks (by design, on a standby
        // replica) until failover, and must never block Spring's context refresh / the actuator
        // port from coming up (§14: actuator on its own port/thread pool, independent of this).
        startupThread = new Thread(this::runStartupSequence, "sequencer-startup");
        startupThread.setDaemon(true);
        startupThread.start();
    }

    private void runStartupSequence() {
        verifyEgressConnected();
        ensureKvBucketExists();
        leaderElection.startAndAwaitLeadership(this::onStartLeading, this::onStopLeading);
    }

    private void verifyEgressConnected() {
        if (ingressConnection.getStatus() != Connection.Status.CONNECTED) {
            throw new IllegalStateException("Ingress NATS connection is not CONNECTED");
        }
        if (egressConnection.getStatus() != Connection.Status.CONNECTED) {
            throw new IllegalStateException("Egress NATS connection is not CONNECTED");
        }
    }

    private void ensureKvBucketExists() {
        try {
            kvManagement.create(KeyValueConfiguration.builder(kvBucket).storageType(StorageType.File).build());
        } catch (JetStreamApiException alreadyExistsOrRaced) {
            // Idempotent: another instance (or a previous run of this one) already created it.
        } catch (IOException e) {
            throw new IllegalStateException("Failed to ensure allocator KV bucket exists: " + kvBucket, e);
        }
    }

    private void onStartLeading() {
        allocator.initialize();
        if (heartbeat.isEnabled()) {
            heartbeatScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "sequencer-heartbeat");
                t.setDaemon(true);
                return t;
            });
            long intervalMs = heartbeat.getIntervalMillis();
            heartbeatScheduler.scheduleAtFixedRate(this::emitHeartbeat, intervalMs, intervalMs, TimeUnit.MILLISECONDS);
        }
        ingress.start(this::handleIngressMessage);
        ingressSubscribed.set(true);
    }

    private void onStopLeading() {
        // Drain-stop ingress FIRST, then stop publishing, then drop readiness (§10).
        ingress.stop();
        ingressSubscribed.set(false);
        if (heartbeatScheduler != null) {
            heartbeatScheduler.shutdownNow();
        }
        destination.flush();
    }

    private void handleIngressMessage(byte[] data) {
        long startNanos = System.nanoTime();
        boolean stamped = stampAndPublish(data);
        if (stamped) {
            metrics.recordLatencyNanos(System.nanoTime() - startNanos);
        }
    }

    private void emitHeartbeat() {
        long highWater;
        synchronized (hotPathLock) {
            highWater = allocator.lastAssigned();
        }
        byte[] message = heartbeat.buildMessage(highWater);
        stampAndPublish(message);
    }

    /** @return {@code true} if the message passed the schemaId guard and was published. */
    private boolean stampAndPublish(byte[] data) {
        synchronized (hotPathLock) {
            buffer.wrap(data);
            long sequenceId = allocator.next();
            boolean ok = stamper.stamp(buffer, sequenceId);
            if (!ok) {
                if (!firstMessageProcessed) {
                    readinessFailed.set(true);
                }
                firstMessageProcessed = true;
                return false;
            }
            firstMessageProcessed = true;
            destination.publish(buffer, 0, data.length);
            metrics.onMessagePublished(data.length, sequenceId);
            return true;
        }
    }

    @Override
    public void stop(Runnable callback) {
        if (running.compareAndSet(true, false)) {
            if (startupThread != null) {
                startupThread.interrupt();
            }
            onStopLeading();
            destination.stop();
            allocator.stop();
            leaderElection.stop();
        }
        callback.run();
    }

    @Override
    public void stop() {
        stop(() -> { });
    }

    @Override
    public boolean isRunning() {
        return running.get();
    }

    /** Returns whether this replica currently holds leadership and has subscribed ingress. */
    public boolean isActive() {
        return leaderElection.isLeader() && ingressSubscribed.get();
    }

    /** Returns {@code false} if the first message ever received failed the schemaId sanity guard. */
    public boolean isSchemaValid() {
        return !readinessFailed.get();
    }

    @Override
    public boolean isAutoStartup() {
        return true;
    }

    @Override
    public int getPhase() {
        return Integer.MAX_VALUE - 1;
    }
}
