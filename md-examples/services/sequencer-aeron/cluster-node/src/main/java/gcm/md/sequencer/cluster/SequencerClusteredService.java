package gcm.md.sequencer.cluster;

import gcm.md.sequencer.stamping.EngineListener;
import gcm.md.sequencer.stamping.StampingConfig;
import gcm.md.sequencer.stamping.StampingEngine;
import gcm.md.sequencer.stamping.Verdict;
import io.aeron.ExclusivePublication;
import io.aeron.Image;
import io.aeron.cluster.codecs.CloseReason;
import io.aeron.cluster.service.Cluster;
import io.aeron.cluster.service.ClusteredService;
import io.aeron.cluster.service.ClientSession;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.UnsafeBuffer;

import java.util.List;

/**
 * The Aeron Cluster member's clustered service (design §5.1) — hosts {@link StampingEngine} and
 * is the phase-2 equivalent of phase-1's {@code SequencerPipeline}: this class owns cluster
 * lifecycle/session/timer plumbing, the engine owns the actual stamping logic.
 *
 * <p><b>Never mutates the buffer {@code onSessionMessage} is given.</b> That buffer is a view
 * onto Aeron's replicated log — every replica processes the identical bytes to reach the
 * identical result deterministically (design §12.3), and the log itself must remain exactly what
 * was proposed. The engine's stamped output is therefore always written into a private,
 * reusable, allocation-free scratch buffer and only that copy is handed to {@link EgressPublisher}.
 */
public final class SequencerClusteredService implements ClusteredService {

    private static final long HEARTBEAT_TIMER_CORRELATION_ID = 1L;
    private static final int HEARTBEAT_SCRATCH_LENGTH = 256;

    private final StampingEngine engine;
    private final SourcePrincipalRegistry principals;
    private final EgressPublisher egressPublisher;
    private final ClusterServiceListener serviceListener;
    private final long heartbeatIntervalNanos;

    private final ExpandableArrayBuffer messageScratch = new ExpandableArrayBuffer(1024);
    private final UnsafeBuffer heartbeatScratch = new UnsafeBuffer(new byte[HEARTBEAT_SCRATCH_LENGTH]);

    private Cluster cluster;
    private boolean heartbeatScheduled;

    public SequencerClusteredService(StampingConfig stampingConfig, List<SourcePrincipal> sources,
                                      long heartbeatIntervalNanos, EgressPublisher egressPublisher,
                                      EngineListener engineListener, ClusterServiceListener serviceListener) {
        this.engine = new StampingEngine(stampingConfig, engineListener);
        this.principals = new SourcePrincipalRegistry(sources);
        this.heartbeatIntervalNanos = heartbeatIntervalNanos;
        this.egressPublisher = egressPublisher;
        this.serviceListener = serviceListener;
    }

    @Override
    public void onStart(Cluster cluster, Image snapshotImage) {
        this.cluster = cluster;
        egressPublisher.onStart(cluster);
        if (snapshotImage != null) {
            engine.loadSnapshot(ClusterSnapshotIO.Reader.drain(snapshotImage));
        }
        // Deliberately does not schedule the heartbeat timer here — see the class Javadoc's
        // note on Aeron's scheduling restriction.
    }

    @Override
    public void onSessionOpen(ClientSession session, long timestamp) {
        boolean admitted = principals.admit(session.id(), session.encodedPrincipal());
        if (!admitted) {
            session.close();
        }
    }

    @Override
    public void onSessionClose(ClientSession session, long timestamp, CloseReason closeReason) {
        principals.onSessionClose(session.id());
    }

    @Override
    public void onSessionMessage(ClientSession session, long timestamp, DirectBuffer buffer, int offset,
                                  int length, Header header) {
        ensureHeartbeatScheduled(timestamp);
        long sourceId = principals.sourceIdFor(session.id());
        messageScratch.putBytes(0, buffer, offset, length);
        // timestamp is this message's own deterministic cluster-log time (Aeron's javadoc:
        // "for when the message was received") — using it directly instead of a redundant
        // cluster.time() call.
        Verdict verdict = engine.onMessage(messageScratch, 0, length, timestamp, sourceId);
        if (verdict == Verdict.STAMPED) {
            egressPublisher.publish(messageScratch, 0, length, engine.currentSequenceId());
        }
        serviceListener.onCommitPositionSample(cluster.logPosition());
    }

    @Override
    public void onTimerEvent(long correlationId, long timestamp) {
        if (correlationId != HEARTBEAT_TIMER_CORRELATION_ID) {
            return;
        }
        int length = engine.onHeartbeatTimer(heartbeatScratch, timestamp);
        egressPublisher.publish(heartbeatScratch, 0, length, engine.currentSequenceId());
        scheduleNextHeartbeat(timestamp);
    }

    @Override
    public void onTakeSnapshot(ExclusivePublication snapshotPublication) {
        // System.nanoTime() here is a pure wall-clock duration measurement for metrics only — it
        // never feeds into engine state or anything written to the replicated log/snapshot, so it
        // doesn't violate design §12.3's determinism requirement despite this class's general
        // wall-clock ban (see ClusterNodeDeterminismRulesTest's Javadoc for that rule's actual
        // scope: replicated output, not local instrumentation).
        long startNanos = System.nanoTime();
        ClusterSnapshotIO.Writer writer = new ClusterSnapshotIO.Writer();
        engine.writeSnapshot(writer);
        writer.flushTo(snapshotPublication);
        serviceListener.onSnapshotTaken(System.nanoTime() - startNanos);
    }

    @Override
    public void onRoleChange(Cluster.Role newRole) {
        egressPublisher.onRoleChange(newRole);
        serviceListener.onRoleChange(newRole);
        if (newRole != Cluster.Role.LEADER) {
            // Lost (or never held) leadership: forget any prior scheduling so a future
            // reassumption of leadership schedules a fresh heartbeat once traffic resumes.
            heartbeatScheduled = false;
        }
    }

    @Override
    public void onTerminate(Cluster cluster) {
        egressPublisher.onTerminate();
    }

    /**
     * Bootstraps the very first heartbeat timer. Aeron only permits {@code scheduleTimer} from
     * {@link #onSessionMessage} or {@link #onTimerEvent} — every lifecycle callback
     * ({@code onStart}, {@code onRoleChange}, {@code doBackgroundWork}) throws
     * {@code ClusterException: sending messages or scheduling timers is not allowed from ...} if
     * called there (confirmed against a live cluster's {@code ClusterTool errors} output — not
     * documented in the Aeron javadoc). So the first heartbeat can only be bootstrapped
     * opportunistically from the first ingress message this replica sees as leader; from then on
     * {@link #onTimerEvent} keeps rescheduling itself. Consequence: on a fully idle cluster with
     * no ingress traffic at all, no heartbeat is ever emitted — acceptable for now since every
     * real deployment has ingress traffic, but a real gap if perfect silence needs to be
     * distinguished from an outage before the first message ever arrives.
     */
    private void ensureHeartbeatScheduled(long timeNanos) {
        if (!heartbeatScheduled && cluster.role() == Cluster.Role.LEADER) {
            scheduleNextHeartbeat(timeNanos);
            heartbeatScheduled = true;
        }
    }

    private void scheduleNextHeartbeat(long fromTimeNanos) {
        cluster.scheduleTimer(HEARTBEAT_TIMER_CORRELATION_ID, fromTimeNanos + heartbeatIntervalNanos);
    }
}
