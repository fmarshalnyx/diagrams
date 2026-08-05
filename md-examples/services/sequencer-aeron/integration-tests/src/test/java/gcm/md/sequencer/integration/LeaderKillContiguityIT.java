package gcm.md.sequencer.integration;

import gcm.md.sequencer.cluster.EgressPublisher;
import gcm.md.sequencer.clusterclient.ClusterIngressClient;
import gcm.md.sequencer.stamping.EngineListener;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

import java.nio.ByteOrder;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.LockSupport;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Design §12.2's centerpiece, upgraded from phase-1's "gap ≤ block-size" tolerance: kill the
 * leader under load; the new leader continues with <b>no gap, no duplicate, no regression</b> on
 * recorded egress. Phase-2's non-negotiable requirement (design §2): sequenceIds are contiguous,
 * not just monotonic.
 *
 * <p>{@code @Disabled} — see this module's pom.xml confidence note: written against the same
 * verified Aeron API that got a real single-member cluster running in Kubernetes, but the
 * multi-member Raft election/failover path here has never actually been run. Validate in the
 * Kubernetes phase before trusting it; expect to need several iterations against real timing
 * behavior, exactly as the single-member path did.
 */
class LeaderKillContiguityIT {

    private static final int MEMBER_COUNT = 3;
    private static final String CREDENTIAL = InProcessCluster.SOURCE_CREDENTIAL;

    @Test
    void killingTheLeaderMidStreamLeavesNoGapNoDuplicateNoRegression() throws Exception {
        Map<Integer, RecordingEgressPublisher> egressByMember = new HashMap<>();
        EgressPublisher[] publishers = new EgressPublisher[MEMBER_COUNT];
        for (int i = 0; i < MEMBER_COUNT; i++) {
            RecordingEgressPublisher recorder = new RecordingEgressPublisher();
            egressByMember.put(i, recorder);
            publishers[i] = recorder;
        }

        String baseDataDir = System.getProperty("java.io.tmpdir") + "/gcm-md-it-leaderkill-" + System.nanoTime();
        try (InProcessCluster cluster = InProcessCluster.start(MEMBER_COUNT, baseDataDir,
                memberId -> publishers[memberId], new EngineListener() {
        })) {
            int leaderBefore = awaitLeader(egressByMember, -1);

            try (ClusterIngressClient client = TestIngressClients.connect(MEMBER_COUNT, CREDENTIAL)) {
                offerMessages(client, 1, 500);

                cluster.killMember(leaderBefore);

                // killMember is a graceful close of the member's whole process (see
                // InProcessCluster#killMember's Javadoc), not a role demotion — the killed
                // member's RecordingEgressPublisher never gets an onRoleChange(FOLLOWER) call, so
                // its isLeader() flag would otherwise stay stuck at true forever. Excluding
                // leaderBefore here (rather than trusting its now-stale recorder) was a real bug
                // caught by actually running this suite: without the exclusion, awaitLeader
                // immediately "re-discovers" the dead member as leader.
                int leaderAfter = awaitLeader(egressByMember, leaderBefore);
                assertThat(leaderAfter).as("a new leader was elected").isNotEqualTo(leaderBefore);

                offerMessages(client, 501, 1000);
            }

            // The critical invariant: across the whole recorded egress (from whichever member(s)
            // were leader at each point), sequenceIds are contiguous from 1, with no duplicate.
            // leaderBefore's recorder is permanently stale (see the exclusion note above) — still
            // excluded here for the same reason.
            RecordingEgressPublisher finalLeaderRecorder = egressByMember.get(awaitLeader(egressByMember, leaderBefore));
            java.util.List<Long> published = finalLeaderRecorder.publishedSequenceIds();
            Set<Long> seen = new HashSet<>(published);
            assertThat(seen).as("no duplicate sequenceIds").hasSameSizeAs(published);

            long expected = 1L;
            for (long sequenceId : published) {
                assertThat(sequenceId).as("no gap, no regression").isEqualTo(expected);
                expected++;
            }
        }
    }

    private static void offerMessages(ClusterIngressClient client, long fromSourceSeqNum, long toSourceSeqNum) {
        for (long seqNum = fromSourceSeqNum; seqNum <= toSourceSeqNum; seqNum++) {
            UnsafeBuffer buffer = new UnsafeBuffer(new byte[68]); // sourceSeqNum at abs offset 64 (int)
            buffer.putShort(2, (short) 9, ByteOrder.LITTLE_ENDIAN); // templateId 9, source-tracked
            buffer.putShort(4, (short) 100, ByteOrder.LITTLE_ENDIAN); // schemaId
            buffer.putInt(64, (int) seqNum, ByteOrder.LITTLE_ENDIAN); // sourceSeqNum
            long result;
            do {
                result = client.offer(buffer, 0, buffer.capacity());
                if (result < 0) {
                    LockSupport.parkNanos(1_000_000L);
                }
            } while (result < 0);
        }
    }

    /**
     * {@code excludedMemberId} (or {@code -1} for none) skips a member's recorder entirely — a
     * killed member's {@link RecordingEgressPublisher#isLeader()} has no way to ever learn it was
     * killed (see the call site's note), so it would otherwise report a permanently stale
     * {@code true}.
     */
    private static int awaitLeader(Map<Integer, RecordingEgressPublisher> egressByMember, int excludedMemberId)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + 30_000L;
        while (System.currentTimeMillis() < deadline) {
            for (Map.Entry<Integer, RecordingEgressPublisher> entry : egressByMember.entrySet()) {
                if (entry.getKey() != excludedMemberId && entry.getValue().isLeader()) {
                    return entry.getKey();
                }
            }
            Thread.sleep(100L);
        }
        throw new IllegalStateException("No leader elected within timeout");
    }
}
