package gcm.md.sequencer.cluster;

import io.aeron.ChannelUriStringBuilder;
import io.aeron.ExclusivePublication;
import io.aeron.Publication;
import io.aeron.archive.client.AeronArchive;
import io.aeron.cluster.service.Cluster;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;

import java.util.concurrent.locks.LockSupport;

/**
 * The real Aeron egress implementation (design §6): a single MDC dynamic publication, recorded
 * by the local Archive, gated by {@link SuppressionGate} so replay/failover never re-emits an
 * already-published message. Replaces {@link NoOpEgressPublisher} from Milestone 2.
 *
 * <p>Each leadership term acquires a <em>fresh</em> Archive recording rather than extending the
 * previous one (design §6.3 wants continuity for downstream replay/DR tooling; that's an
 * operational nicety deferred here, not a correctness requirement). {@link #EGRESS_SESSION_ID}
 * is a fixed, well-known session id used on every acquisition specifically so
 * {@link ArchiveRecordingTailQuery} can find "the most recent recording of this stream"
 * ({@code findLastMatchingRecording}) across restarts and leader changes without any extra
 * cross-instance bookkeeping — the suppression gate's correctness rests on that lookup finding
 * the right recording, not on the recording being physically continuous.
 */
public final class AeronEgressPublisher implements EgressPublisher {

    static final int EGRESS_SESSION_ID = 1_000_100;

    private final AeronEgressConfig config;
    private final AeronArchive.Context archiveClientContextTemplate;
    private final EgressListener listener;
    private final SuppressionGate gate = new SuppressionGate();

    private Cluster cluster;
    private AeronArchive archive;
    private ExclusivePublication publication;

    public AeronEgressPublisher(AeronEgressConfig config, AeronArchive.Context archiveClientContextTemplate,
                                 EgressListener listener) {
        this.config = config;
        this.archiveClientContextTemplate = archiveClientContextTemplate;
        this.listener = listener;
    }

    @Override
    public void onStart(Cluster cluster) {
        this.cluster = cluster;
        this.archive = AeronArchive.connect(archiveClientContextTemplate.clone()
                .aeron(cluster.aeron())
                .ownsAeronClient(false));
    }

    @Override
    public void onRoleChange(Cluster.Role newRole) {
        if (newRole == Cluster.Role.LEADER) {
            long lastPublished = new ArchiveRecordingTailQuery(archive, cluster.aeron(), config).lastPublishedSequenceId();
            gate.onRoleChange(Cluster.Role.LEADER, lastPublished);
            acquirePublication();
        } else {
            gate.onRoleChange(newRole, SuppressionGate.NO_SUPPRESSION);
            closePublication();
        }
    }

    @Override
    public void publish(DirectBuffer buffer, int offset, int length, long sequenceId) {
        if (gate.shouldSuppress(sequenceId)) {
            listener.onSuppressed(sequenceId);
            return;
        }
        offerWithBackpressureRetry(buffer, offset, length);
    }

    @Override
    public void onTerminate() {
        closePublication();
        CloseHelper.quietClose(archive);
    }

    private void acquirePublication() {
        closePublication();
        String channelWithSessionId = new ChannelUriStringBuilder(config.egressChannel())
                .sessionId(EGRESS_SESSION_ID)
                .build();
        publication = archive.addRecordedExclusivePublication(channelWithSessionId, config.egressStreamId());
    }

    private void closePublication() {
        CloseHelper.quietClose(publication);
        publication = null;
    }

    private void offerWithBackpressureRetry(DirectBuffer buffer, int offset, int length) {
        long stallStartNanos = 0L;
        int spins = 0;
        while (true) {
            long result = publication.offer(buffer, offset, length);
            if (result >= 0L) {
                if (stallStartNanos != 0L) {
                    listener.onBackpressureStall(System.nanoTime() - stallStartNanos);
                }
                return;
            }
            if (result == Publication.NOT_CONNECTED || result == Publication.CLOSED
                    || result == Publication.MAX_POSITION_EXCEEDED) {
                // MDC with zero subscribers is not blockage (design §6.4, config
                // egress.linger-on-no-subscribers) — nothing to retry against. Phase-1's `drop`
                // mode is deleted (contiguous sequences would make it violate design §2), but a
                // disconnected destination with no subscribers has no queue to apply backpressure
                // against either; this attempt is simply not delivered to anyone.
                return;
            }
            // BACK_PRESSURED or ADMIN_ACTION: bounded spin, then idle — unbounded overall retry,
            // matching phase-1 §9 `block` semantics (stall rather than silent, unreplayable loss).
            if (stallStartNanos == 0L) {
                stallStartNanos = System.nanoTime();
            }
            spins++;
            if (spins < config.backpressureMaxSpins()) {
                Thread.onSpinWait();
            } else {
                LockSupport.parkNanos(config.backpressureIdleNanos());
            }
        }
    }
}
