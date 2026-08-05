package gcm.md.natsbridge.bridge;

import gcm.md.natsbridge.config.NatsBridgeProperties;
import gcm.md.natsbridge.metrics.BridgeMetrics;
import gcm.md.sequencer.egress.DestinationChannel;
import io.aeron.Aeron;
import io.aeron.Image;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.ReplayMerge;
import io.aeron.logbuffer.FragmentHandler;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.SystemEpochClock;
import org.springframework.context.SmartLifecycle;

import java.nio.ByteOrder;
import java.util.concurrent.TimeUnit;

/**
 * Subscribes to the sequenced Aeron egress and republishes to {@code MD_SEQUENCED} (design §9).
 * Uses Aeron's built-in {@link ReplayMerge} to catch up from the leader's Archive recording after
 * downtime, then transparently continues on the live {@link Image} once merged — the standard
 * Aeron pattern for exactly this "full history of a stream" requirement, rather than hand-rolled
 * replay bookkeeping.
 *
 * <p>The bridge always replays its target recording from the very start rather than computing an
 * exact resume byte-position from its NATS KV checkpoint (design §9: "stateless-restartable,
 * never authoritative") — {@link ContiguityTracker} cheaply skips everything at or before the
 * checkpoint, which avoids needing to translate a sequenceId into an exact Archive position (a
 * translation this module has no clean way to perform) at the cost of a bounded amount of replay
 * work on every restart. Recordings are retention-bounded (design §6.3), so this is bounded, not
 * unbounded, replay.
 *
 * <p><b>Confidence note:</b> like {@code cluster-node}'s {@code ArchiveRecordingTailQuery}, the
 * {@link #run()} driving loop here is written against the verified Aeron Archive/ReplayMerge
 * client API but has not been exercised against a live Archive/cluster. {@link #onFragment} — the
 * actual skip/bridge/gap/checkpoint decision logic — is separated out specifically so it can be
 * (and is) unit-tested without any live Aeron dependency; see {@code BridgePipelineTest}.
 */
public final class BridgePipeline implements SmartLifecycle {

    /**
     * Must match {@code cluster-node}'s {@code AeronEgressPublisher.EGRESS_SESSION_ID} exactly.
     * Public: {@code ServiceConfiguration}'s {@code LeaderArchiveConnector} wiring also needs
     * this value now, to find which member has the matching recording before this class exists.
     */
    public static final int EGRESS_SESSION_ID = 1_000_100;

    private static final ByteOrder LE = ByteOrder.LITTLE_ENDIAN;
    private static final int POLL_FRAGMENT_LIMIT = 10;

    private final Aeron aeron;
    private final AeronArchive archive;
    private final NatsBridgeProperties.Cluster clusterConfig;
    private final NatsBridgeProperties.Stamping stampingConfig;
    private final DestinationChannel destination;
    private final BridgeCheckpoint checkpoint;
    private final BridgeMetrics metrics;
    private final int checkpointIntervalMessages;
    private final ContiguityTracker tracker;

    private int messagesSinceCheckpoint;
    private volatile boolean running;
    private Thread bridgeThread;

    public BridgePipeline(Aeron aeron, AeronArchive archive, NatsBridgeProperties properties,
                           DestinationChannel destination, BridgeCheckpoint checkpoint, BridgeMetrics metrics) {
        this.aeron = aeron;
        this.archive = archive;
        this.clusterConfig = properties.getCluster();
        this.stampingConfig = properties.getStamping();
        this.destination = destination;
        this.checkpoint = checkpoint;
        this.metrics = metrics;
        this.checkpointIntervalMessages = properties.getNats().getCheckpointIntervalMessages();
        this.tracker = new ContiguityTracker(checkpoint.read(),
                properties.getNats().getCheckpointResetThresholdMessages());
    }

    @Override
    public void start() {
        running = true;
        bridgeThread = new Thread(this::run, "nats-bridge");
        bridgeThread.setDaemon(true);
        bridgeThread.start();
    }

    @Override
    public void stop() {
        running = false;
        if (bridgeThread != null) {
            bridgeThread.interrupt();
        }
        destination.stop();
        CloseHelper.quietClose(archive);
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    private void run() {
        FragmentHandler handler = (buffer, offset, length, header) -> onFragment(buffer, offset, length);
        long recordingId = archive.findLastMatchingRecording(0L, clusterConfig.getLiveDestination(),
                clusterConfig.getEgressStreamId(), EGRESS_SESSION_ID);

        if (recordingId == Aeron.NULL_VALUE) {
            // Nothing recorded yet (fresh cluster) — nothing to catch up on, subscribe live only.
            Subscription liveOnly = aeron.addSubscription(clusterConfig.getLiveDestination(),
                    clusterConfig.getEgressStreamId());
            pollUntilStopped(() -> liveOnly.poll(handler, POLL_FRAGMENT_LIMIT));
            return;
        }

        Subscription subscription = aeron.addSubscription(clusterConfig.getSubscriptionChannel(),
                clusterConfig.getEgressStreamId());
        ReplayMerge replayMerge = new ReplayMerge(subscription, archive, clusterConfig.getReplayChannel(),
                clusterConfig.getReplayDestination(), clusterConfig.getLiveDestination(), recordingId, 0L,
                SystemEpochClock.INSTANCE, TimeUnit.SECONDS.toMillis(5));
        metrics.setReplayCatchUpActive(true);
        try {
            while (running && !replayMerge.isMerged()) {
                try {
                    if (replayMerge.poll(handler, POLL_FRAGMENT_LIMIT) <= 0) {
                        Thread.onSpinWait();
                    }
                } catch (RuntimeException transientFailure) {
                    // See pollUntilStopped's identical catch for why this must not propagate.
                    metrics.onLoopError();
                    System.err.println("nats-bridge: replay-merge poll iteration failed, retrying: "
                            + transientFailure);
                }
            }
        } finally {
            Image image = replayMerge.image();
            // Safe once merged (or the loop exited on stop()/failure): the Image/Subscription
            // continue independently of the ReplayMerge instance per its documented contract.
            replayMerge.close();
            metrics.setReplayCatchUpActive(false);
            if (running && image != null) {
                pollUntilStopped(() -> image.poll(handler, POLL_FRAGMENT_LIMIT));
            }
        }
    }

    private interface PollAttempt {
        int poll();
    }

    private void pollUntilStopped(PollAttempt attempt) {
        while (running) {
            try {
                if (attempt.poll() <= 0) {
                    Thread.onSpinWait();
                }
            } catch (RuntimeException transientFailure) {
                // A single poll (which synchronously invokes onFragment, including its
                // checkpoint.write() call) failing must not kill this daemon thread permanently:
                // there's no supervisor, so an uncaught exception here would silently and
                // permanently stop bridging while isRunning()/the readiness probe both keep
                // reporting healthy — a real incident this session (a transient NATS JetStream
                // "insufficient resources" error on a checkpoint write took the whole bridge
                // offline with no restart and no visible failure). Nothing was acked/consumed
                // past this fragment on failure, so the next successful poll picks up where this
                // one left off; just log, count, and keep polling.
                metrics.onLoopError();
                System.err.println("nats-bridge: poll iteration failed, retrying: " + transientFailure);
            }
        }
    }

    /**
     * The actual per-fragment decision: skip if already bridged, else publish and checkpoint.
     * Package-private and free of any live Aeron dependency so it's directly unit-testable.
     */
    void onFragment(DirectBuffer buffer, int offset, int length) {
        long sequenceId = buffer.getLong(offset + stampingConfig.getSequenceIdOffset(), LE);
        ContiguityTracker.Evaluation evaluation = tracker.evaluate(sequenceId);
        if (evaluation.decision() == ContiguityTracker.Decision.SKIP_ALREADY_BRIDGED) {
            return;
        }
        if (evaluation.decision() == ContiguityTracker.Decision.GAP_BUT_BRIDGE) {
            metrics.onGapDetected(evaluation.gapSize());
        }
        if (evaluation.decision() == ContiguityTracker.Decision.CHECKPOINT_RESET_AND_BRIDGE) {
            metrics.onCheckpointReset();
        }

        destination.publish(buffer, offset, length);
        metrics.onMessageBridged(sequenceId);

        if (++messagesSinceCheckpoint >= checkpointIntervalMessages) {
            checkpoint.write(sequenceId);
            messagesSinceCheckpoint = 0;
        }
    }
}
