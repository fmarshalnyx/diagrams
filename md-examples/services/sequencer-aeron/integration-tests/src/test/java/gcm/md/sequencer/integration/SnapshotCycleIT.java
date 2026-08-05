package gcm.md.sequencer.integration;

import gcm.md.sequencer.cluster.EgressPublisher;
import gcm.md.sequencer.clusterclient.ClusterIngressClient;
import gcm.md.sequencer.stamping.EngineListener;
import io.aeron.cluster.ClusterTool;
import io.aeron.cluster.RecordingLog;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.ByteOrder;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.LockSupport;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Design §12.2: force a snapshot mid-load, restart a member, assert its state after
 * restart-from-snapshot matches a continuously-running member's state — i.e. loading a snapshot
 * and replaying only the log tail after it must be equivalent to replaying the whole log from the
 * start. This is the cluster-level counterpart of
 * {@code DeterminismTest#snapshotEquivalence_snapshotMidwayThenResumeMatchesContinuousReplay()} in
 * {@code sequencer-core}, which already proves the same property for the bare
 * {@code StampingEngine} outside of Aeron. This test proves the Aeron plumbing around it —
 * {@link ClusterTool#snapshot}, {@code onTakeSnapshot}/the snapshot image replay path in
 * {@code SequencerClusteredService} — doesn't break that guarantee.
 *
 * <p>Compares high-water-mark sequenceIds (not full byte-identical output, which
 * {@code DeterminismTest} already covers) because {@link RecordingEgressPublisher} is unguarded
 * by role or replay-position, so a member's recorded list can contain re-emitted entries across a
 * restart — see {@link FollowerRescheduleIT}'s Javadoc for why that's fine for this suite's
 * purpose.
 *
 * <p>{@code @Disabled} — see this module's pom.xml confidence note.
 */
class SnapshotCycleIT {

    private static final int MEMBER_COUNT = 3;
    private static final String CREDENTIAL = InProcessCluster.SOURCE_CREDENTIAL;

    @Test
    void restartFromSnapshotReachesTheSameStateAsAContinuouslyRunningMember() throws Exception {
        Map<Integer, RecordingEgressPublisher> egressByMember = new HashMap<>();
        EgressPublisher[] publishers = new EgressPublisher[MEMBER_COUNT];
        for (int i = 0; i < MEMBER_COUNT; i++) {
            RecordingEgressPublisher recorder = new RecordingEgressPublisher();
            egressByMember.put(i, recorder);
            publishers[i] = recorder;
        }

        String baseDataDir = System.getProperty("java.io.tmpdir") + "/gcm-md-it-snapshotcycle-" + System.nanoTime();
        try (InProcessCluster cluster = InProcessCluster.start(MEMBER_COUNT, baseDataDir,
                memberId -> publishers[memberId], new EngineListener() {
        })) {
            int leader = awaitLeader(egressByMember);
            int restartedMember = (leader + 1) % MEMBER_COUNT;
            int controlMember = (leader + 2) % MEMBER_COUNT;

            try (ClusterIngressClient client = TestIngressClients.connect(MEMBER_COUNT, CREDENTIAL)) {
                offerMessages(client, 1, 300);
                awaitHighWaterMark(egressByMember.get(restartedMember), 300L);

                boolean snapshotRequested = ClusterTool.snapshot(
                        new File(cluster.clusterDirectoryName(leader)), System.out);
                assertThat(snapshotRequested).as("ClusterTool accepted the snapshot request").isTrue();
                awaitSnapshotTaken(new File(cluster.clusterDirectoryName(leader)));

                cluster.killMember(restartedMember);
                offerMessages(client, 301, 700); // traffic continues past the snapshot while the member is down

                cluster.restartMember(restartedMember);
                awaitHighWaterMark(egressByMember.get(restartedMember), 700L);
            }

            long controlHighWaterMark = highWaterMark(egressByMember.get(controlMember));
            long restartedHighWaterMark = highWaterMark(egressByMember.get(restartedMember));
            assertThat(restartedHighWaterMark)
                    .as("member restarted from snapshot reaches the same final state as a continuously-running member")
                    .isEqualTo(controlHighWaterMark);
        }
    }

    private static long highWaterMark(RecordingEgressPublisher recorder) {
        List<Long> published = recorder.publishedSequenceIds();
        return published.isEmpty() ? 0L : published.get(published.size() - 1);
    }

    private static void awaitHighWaterMark(RecordingEgressPublisher recorder, long expected) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 30_000L;
        while (System.currentTimeMillis() < deadline) {
            if (highWaterMark(recorder) >= expected) {
                return;
            }
            Thread.sleep(100L);
        }
        throw new IllegalStateException("Replica did not reach sequenceId " + expected + " within timeout");
    }

    /**
     * {@link ClusterTool#snapshot} only enqueues the request; poll the on-disk recording log
     * (service id 0 — {@code SequencerClusteredService} is the cluster's only clustered service)
     * until an entry actually lands.
     */
    private static void awaitSnapshotTaken(File clusterDir) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 30_000L;
        while (System.currentTimeMillis() < deadline) {
            try (RecordingLog recordingLog = new RecordingLog(clusterDir, false)) {
                if (recordingLog.getLatestSnapshot(0) != null) {
                    return;
                }
            }
            Thread.sleep(200L);
        }
        throw new IllegalStateException("Snapshot did not complete within timeout");
    }

    private static void offerMessages(ClusterIngressClient client, long fromSourceSeqNum, long toSourceSeqNum) {
        for (long seqNum = fromSourceSeqNum; seqNum <= toSourceSeqNum; seqNum++) {
            UnsafeBuffer buffer = new UnsafeBuffer(new byte[68]); // sourceSeqNum at abs offset 64 (int)
            buffer.putShort(2, (short) 9, ByteOrder.LITTLE_ENDIAN);
            buffer.putShort(4, (short) 100, ByteOrder.LITTLE_ENDIAN);
            buffer.putInt(64, (int) seqNum, ByteOrder.LITTLE_ENDIAN);
            long result;
            do {
                result = client.offer(buffer, 0, buffer.capacity());
                if (result < 0) {
                    LockSupport.parkNanos(1_000_000L);
                }
            } while (result < 0);
        }
    }

    private static int awaitLeader(Map<Integer, RecordingEgressPublisher> egressByMember) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 30_000L;
        while (System.currentTimeMillis() < deadline) {
            for (Map.Entry<Integer, RecordingEgressPublisher> entry : egressByMember.entrySet()) {
                if (entry.getValue().isLeader()) {
                    return entry.getKey();
                }
            }
            Thread.sleep(100L);
        }
        throw new IllegalStateException("No leader elected within timeout");
    }
}
