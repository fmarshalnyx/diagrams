package gcm.md.sequencer.cluster;

import gcm.md.sequencer.stamping.EngineListener;
import gcm.md.sequencer.stamping.StampingConfig;
import io.aeron.ExclusivePublication;
import io.aeron.cluster.service.ClientSession;
import io.aeron.cluster.service.Cluster;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link SequencerClusteredService}'s callback logic against Mockito fakes for
 * {@code Cluster}/{@code ClientSession} — no live Aeron cluster networking. This exercises the
 * design §5 acceptance scenario ("accepts a session, stamps a canned message end-to-end into an
 * in-memory sink") at the logic level; a real embedded single-member cluster runtime test
 * belongs to {@code services/sequencer-aeron/integration-tests} (design §12.2), not here.
 */
class SequencerClusteredServiceTest {

    private static final long SOURCE_ID = 7L;
    private static final long SESSION_ID = 42L;
    private static final long HEARTBEAT_INTERVAL_NANOS = TimeUnit.MILLISECONDS.toNanos(100);
    private static final ByteOrder LE = ByteOrder.LITTLE_ENDIAN;

    private static final class RecordingEgressPublisher implements EgressPublisher {
        final List<byte[]> published = new ArrayList<>();
        final List<Long> publishedSequenceIds = new ArrayList<>();
        Cluster.Role lastRole;
        boolean terminated;

        @Override
        public void onStart(Cluster cluster) {
        }

        @Override
        public void publish(DirectBuffer buffer, int offset, int length, long sequenceId) {
            byte[] copy = new byte[length];
            buffer.getBytes(offset, copy);
            published.add(copy);
            publishedSequenceIds.add(sequenceId);
        }

        @Override
        public void onRoleChange(Cluster.Role role) {
            lastRole = role;
        }

        @Override
        public void onTerminate() {
            terminated = true;
        }
    }

    private static final class RecordingServiceListener implements ClusterServiceListener {
        final List<Cluster.Role> roleChanges = new ArrayList<>();
        final List<Long> snapshotDurationsNanos = new ArrayList<>();
        final List<Long> commitPositionSamples = new ArrayList<>();

        @Override
        public void onRoleChange(Cluster.Role newRole) {
            roleChanges.add(newRole);
        }

        @Override
        public void onSnapshotTaken(long durationNanos) {
            snapshotDurationsNanos.add(durationNanos);
        }

        @Override
        public void onCommitPositionSample(long position) {
            commitPositionSamples.add(position);
        }
    }

    private final RecordingEgressPublisher egress = new RecordingEgressPublisher();
    private final RecordingServiceListener serviceListener = new RecordingServiceListener();
    private final Cluster cluster = mock(Cluster.class);
    private SequencerClusteredService service;

    @BeforeEach
    void setUp() {
        List<SourcePrincipal> sources = List.of(new SourcePrincipal("test-source", SOURCE_ID, "test-token"));
        service = new SequencerClusteredService(StampingConfig.v4Defaults(), sources, HEARTBEAT_INTERVAL_NANOS,
                egress, new EngineListener() {
        }, serviceListener);
    }

    private ClientSession admittedSession() {
        ClientSession session = mock(ClientSession.class);
        when(session.id()).thenReturn(SESSION_ID);
        when(session.encodedPrincipal()).thenReturn("test-token".getBytes(StandardCharsets.UTF_8));
        service.onSessionOpen(session, 0L);
        return session;
    }

    private UnsafeBuffer untrackedMessage() {
        UnsafeBuffer buffer = new UnsafeBuffer(new byte[64]);
        buffer.putShort(2, (short) 1, LE); // templateId 1: not source-tracked by default config
        buffer.putShort(4, (short) 100, LE); // schemaId
        return buffer;
    }

    @Test
    void firstIngressMessageAsLeaderBootstrapsTheHeartbeatTimer() {
        // Aeron only permits scheduleTimer from onSessionMessage/onTimerEvent — every lifecycle
        // callback (onStart, onRoleChange, doBackgroundWork) throws if it tries (confirmed
        // against a live cluster, not documented) — see the class Javadoc. So the first
        // heartbeat is bootstrapped opportunistically from the first ingress message instead,
        // scheduled relative to that message's own timestamp (design: cluster time, not wall time).
        when(cluster.role()).thenReturn(Cluster.Role.LEADER);
        service.onStart(cluster, null);
        service.onRoleChange(Cluster.Role.LEADER);
        verify(cluster, never()).scheduleTimer(anyLong(), anyLong());

        ClientSession session = admittedSession();
        UnsafeBuffer message = untrackedMessage();
        service.onSessionMessage(session, 1_000L, message, 0, message.capacity(), null);
        verify(cluster).scheduleTimer(anyLong(), eq(1_000L + HEARTBEAT_INTERVAL_NANOS));

        // Only bootstrapped once, not on every subsequent message.
        service.onSessionMessage(session, 2_000L, message, 0, message.capacity(), null);
        verify(cluster, times(1)).scheduleTimer(anyLong(), anyLong());
    }

    @Test
    void ingressMessageWhileNotLeaderDoesNotScheduleAHeartbeat() {
        when(cluster.time()).thenReturn(1_000L);
        when(cluster.role()).thenReturn(Cluster.Role.FOLLOWER);
        service.onStart(cluster, null);
        ClientSession session = admittedSession();
        UnsafeBuffer message = untrackedMessage();
        service.onSessionMessage(session, 0L, message, 0, message.capacity(), null);
        verify(cluster, never()).scheduleTimer(anyLong(), anyLong());
    }

    @Test
    void losingLeadershipAllowsReschedulingOnReassumption() {
        when(cluster.time()).thenReturn(1_000L);
        when(cluster.role()).thenReturn(Cluster.Role.LEADER);
        service.onStart(cluster, null);
        ClientSession session = admittedSession();
        UnsafeBuffer message = untrackedMessage();
        service.onSessionMessage(session, 0L, message, 0, message.capacity(), null);
        verify(cluster, times(1)).scheduleTimer(anyLong(), anyLong());

        service.onRoleChange(Cluster.Role.FOLLOWER);
        service.onRoleChange(Cluster.Role.LEADER);
        service.onSessionMessage(session, 0L, message, 0, message.capacity(), null);
        verify(cluster, times(2)).scheduleTimer(anyLong(), anyLong());
    }

    @Test
    void onSessionOpenAdmitsAConfiguredCredential() {
        ClientSession session = admittedSession();
        verify(session, never()).close();
    }

    @Test
    void onSessionOpenRejectsAnUnknownCredential() {
        ClientSession session = mock(ClientSession.class);
        when(session.id()).thenReturn(SESSION_ID);
        when(session.encodedPrincipal()).thenReturn("not-configured".getBytes(StandardCharsets.UTF_8));
        service.onSessionOpen(session, 0L);
        verify(session).close();
    }

    @Test
    void onSessionMessageStampsAndPublishesOnStampedVerdict() {
        service.onStart(cluster, null);
        ClientSession session = admittedSession();

        UnsafeBuffer buffer = new UnsafeBuffer(new byte[64]);
        buffer.putShort(2, (short) 1, LE); // templateId 1: not source-tracked by default config
        buffer.putShort(4, (short) 100, LE); // schemaId

        service.onSessionMessage(session, 500L, buffer, 0, buffer.capacity(), null);

        assertThat(egress.published).hasSize(1);
        assertThat(egress.publishedSequenceIds).containsExactly(1L);
        UnsafeBuffer published = new UnsafeBuffer(egress.published.get(0));
        assertThat(published.getLong(8, LE)).isEqualTo(1L);   // sequenceId
        assertThat(published.getLong(32, LE)).isEqualTo(500L); // sequenceTimestamp == this message's own timestamp
        // The engine must never mutate the buffer the callback was given (design: it's a view
        // onto the replicated log) — only the private scratch copy that was published.
        assertThat(buffer.getLong(8, LE)).isZero();
    }

    @Test
    void onSessionMessageDoesNotPublishOnDuplicateVerdict() {
        when(cluster.time()).thenReturn(0L);
        service.onStart(cluster, null);
        ClientSession session = admittedSession();

        UnsafeBuffer first = new UnsafeBuffer(new byte[96]);
        first.putShort(2, (short) 9, LE); // templateId 9: source-tracked at abs offset 64 by default
        first.putShort(4, (short) 100, LE);
        first.putInt(64, 1, LE);
        service.onSessionMessage(session, 0L, first, 0, first.capacity(), null);

        UnsafeBuffer replay = new UnsafeBuffer(new byte[96]);
        replay.putShort(2, (short) 9, LE);
        replay.putShort(4, (short) 100, LE);
        replay.putInt(64, 1, LE); // same sourceSeqNum again
        service.onSessionMessage(session, 0L, replay, 0, replay.capacity(), null);

        assertThat(egress.published).hasSize(1); // only the first was published
    }

    @Test
    void onTimerEventEmitsHeartbeatAndReschedules() {
        when(cluster.time()).thenReturn(0L);
        when(cluster.role()).thenReturn(Cluster.Role.LEADER);
        service.onStart(cluster, null);
        service.onRoleChange(Cluster.Role.LEADER);
        ClientSession bootstrapSession = admittedSession();
        UnsafeBuffer bootstrapMessage = untrackedMessage();
        service.onSessionMessage(bootstrapSession, 0L, bootstrapMessage, 0, bootstrapMessage.capacity(), null);

        ArgumentCaptor<Long> correlationId = ArgumentCaptor.forClass(Long.class);
        verify(cluster).scheduleTimer(correlationId.capture(), anyLong());

        int publishedBeforeTimer = egress.published.size(); // the bootstrap message itself was published too
        service.onTimerEvent(correlationId.getValue(), 1_000L);

        assertThat(egress.published).hasSize(publishedBeforeTimer + 1);
        verify(cluster).scheduleTimer(correlationId.getValue(), 1_000L + HEARTBEAT_INTERVAL_NANOS);
    }

    @Test
    void onTimerEventIgnoresAnUnrelatedCorrelationId() {
        when(cluster.time()).thenReturn(0L);
        service.onStart(cluster, null);
        service.onTimerEvent(999_999L, 1_000L);
        assertThat(egress.published).isEmpty();
    }

    @Test
    void onTakeSnapshotOffersTheEngineStateAsOneMessage() {
        ExclusivePublication publication = mock(ExclusivePublication.class);
        when(publication.offer(any(DirectBuffer.class), anyInt(), anyInt())).thenReturn(1L);
        service.onTakeSnapshot(publication);
        verify(publication).offer(any(DirectBuffer.class), eq(0), anyInt());
    }

    @Test
    void onTakeSnapshotReportsItsDurationToTheServiceListener() {
        ExclusivePublication publication = mock(ExclusivePublication.class);
        when(publication.offer(any(DirectBuffer.class), anyInt(), anyInt())).thenReturn(1L);
        service.onTakeSnapshot(publication);
        assertThat(serviceListener.snapshotDurationsNanos).hasSize(1);
        assertThat(serviceListener.snapshotDurationsNanos.get(0)).isGreaterThanOrEqualTo(0L);
    }

    @Test
    void onRoleChangeNotifiesTheEgressPublisher() {
        when(cluster.time()).thenReturn(0L);
        service.onStart(cluster, null);
        service.onRoleChange(Cluster.Role.LEADER);
        assertThat(egress.lastRole).isEqualTo(Cluster.Role.LEADER);
    }

    @Test
    void onRoleChangeNotifiesTheServiceListener() {
        when(cluster.time()).thenReturn(0L);
        service.onStart(cluster, null);
        service.onRoleChange(Cluster.Role.LEADER);
        service.onRoleChange(Cluster.Role.FOLLOWER);
        assertThat(serviceListener.roleChanges).containsExactly(Cluster.Role.LEADER, Cluster.Role.FOLLOWER);
    }

    @Test
    void onSessionMessageSamplesTheCommitPositionToTheServiceListener() {
        when(cluster.logPosition()).thenReturn(12_345L);
        service.onStart(cluster, null);
        ClientSession session = admittedSession();
        UnsafeBuffer message = untrackedMessage();

        service.onSessionMessage(session, 0L, message, 0, message.capacity(), null);

        assertThat(serviceListener.commitPositionSamples).containsExactly(12_345L);
    }

    @Test
    void onTerminateNotifiesTheEgressPublisher() {
        service.onTerminate(cluster);
        assertThat(egress.terminated).isTrue();
    }
}
