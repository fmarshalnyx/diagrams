package gcm.md.sequencer.integration;

import gcm.md.sequencer.cluster.EgressPublisher;
import gcm.md.sequencer.clusterclient.ClusterIngressClient;
import gcm.md.sequencer.stamping.EngineListener;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

import java.nio.ByteOrder;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.LockSupport;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Design §12.2: delete a follower, return with a new IP (Kubernetes pod reschedule), assert
 * rejoin and log catch-up. This in-process harness can't reproduce "new IP" (same JVM, same
 * {@code localhost} port for the restarted member — see
 * {@link gcm.md.sequencer.cluster.ClusterNodeConfig#localMultiMember}'s Javadoc), so it exercises
 * the state-machine-replication half only: every replica, including a follower that was down for
 * a while, must independently reach the identical {@code StampingEngine} state after catch-up —
 * that's what makes it safe for a follower to become leader later. The DNS-re-resolution half of
 * this scenario is a Kubernetes-only concept and belongs to {@code 50-failover-drill.sh}'s
 * eventual scope, not this in-process suite.
 *
 * <p>Uses {@link RecordingEgressPublisher} deliberately unguarded by role — every replica
 * (leader or follower) calls {@code publish()} for every message it stamps, so recorded
 * sequenceIds are a direct proxy for "how far has this replica's engine actually replicated,"
 * independent of the separate egress-gating question {@link EgressNoDoublePublishIT} covers.
 *
 * <p>{@code @Disabled} — see this module's pom.xml confidence note.
 */
class FollowerRescheduleIT {

    private static final int MEMBER_COUNT = 3;
    private static final String CREDENTIAL = InProcessCluster.SOURCE_CREDENTIAL;

    @Test
    void aRestartedFollowerCatchesUpToTheCurrentLogPosition() throws Exception {
        Map<Integer, RecordingEgressPublisher> egressByMember = new HashMap<>();
        EgressPublisher[] publishers = new EgressPublisher[MEMBER_COUNT];
        for (int i = 0; i < MEMBER_COUNT; i++) {
            RecordingEgressPublisher recorder = new RecordingEgressPublisher();
            egressByMember.put(i, recorder);
            publishers[i] = recorder;
        }

        String baseDataDir = System.getProperty("java.io.tmpdir") + "/gcm-md-it-followerreschedule-" + System.nanoTime();
        try (InProcessCluster cluster = InProcessCluster.start(MEMBER_COUNT, baseDataDir,
                memberId -> publishers[memberId], new EngineListener() {
        })) {
            int leader = awaitLeader(egressByMember);
            int follower = (leader + 1) % MEMBER_COUNT;

            try (ClusterIngressClient client = TestIngressClients.connect(MEMBER_COUNT, CREDENTIAL)) {
                offerMessages(client, 1, 200);
                awaitSequenceId(egressByMember.get(follower), 200L);

                cluster.killMember(follower);
                offerMessages(client, 201, 500); // traffic continues while the follower is down

                cluster.restartMember(follower);
                awaitSequenceId(egressByMember.get(follower), 500L); // catch-up
            }
        }
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

    private static void awaitSequenceId(RecordingEgressPublisher recorder, long expectedHighWaterMark)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + 30_000L;
        while (System.currentTimeMillis() < deadline) {
            var published = recorder.publishedSequenceIds();
            if (!published.isEmpty() && published.get(published.size() - 1) >= expectedHighWaterMark) {
                return;
            }
            Thread.sleep(100L);
        }
        throw new IllegalStateException("Replica did not catch up to sequenceId " + expectedHighWaterMark + " within timeout");
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
