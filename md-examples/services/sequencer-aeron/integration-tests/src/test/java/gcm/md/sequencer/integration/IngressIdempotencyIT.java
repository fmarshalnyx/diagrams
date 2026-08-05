package gcm.md.sequencer.integration;

import gcm.md.sequencer.cluster.EgressPublisher;
import gcm.md.sequencer.clusterclient.ClusterIngressClient;
import gcm.md.sequencer.stamping.EngineListener;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

import java.nio.ByteOrder;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.LockSupport;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Design §4 / §12.2: a source client that lost its ack and republishes an overlapping tail of
 * {@code sourceSeqNum}s (design §7's cluster-client at-least-once retry contract, shared by both
 * {@code IngressTransport} implementations) must have every duplicate absorbed by
 * {@code StampingEngine}'s per-source dedup — zero new sequenceIds consumed for already-seen
 * {@code sourceSeqNum}s, per
 * {@link gcm.md.sequencer.stamping.Verdict#DUPLICATE}'s contract.
 * {@code SequencerClusteredService} only calls {@code EgressPublisher.publish} for
 * {@link gcm.md.sequencer.stamping.Verdict#STAMPED}, so a duplicate simply never appears in
 * {@link RecordingEgressPublisher}'s recorded list — this test's assertion is exactly that
 * absence, checked via strictly-increasing sequenceIds with no repeats among this suite's own
 * templateId (a repeat would be visible; a gap is expected and fine — the cluster's own
 * heartbeats interleave real sequenceIds from the same global counter, see
 * {@link RecordingEgressPublisher#sequenceIdsForTemplate}).
 *
 * <p><b>Known-flaky as of this writing</b> — see {@code docs/AERON-SEQUENCER-3-MEMBER-CLUSTER-STATUS.md}.
 * A 300+-message burst offered near-instantaneously can permanently stall partway through: Aeron
 * counters on the leader show its internal {@code alias=log} MDC publication's {@code pub-pos}
 * pinned exactly at its {@code pub-lmt} (a genuine flow-control backpressure stall, not idleness —
 * confirmed via {@code AeronStat}/{@code ErrorStat} showing zero NAKs/invalid-packets/errors at
 * the transport level throughout). Reproduces in a freshly restarted, otherwise-idle environment,
 * so it is not the memory-pressure artifact a prior session suspected. Root cause not yet found;
 * suspect the local Archive recording subscriber (a consumer of the same log stream the flow
 * control gates on) failing to keep advancing under this in-process 3-embedded-driver-in-one-JVM
 * harness specifically. {@link #awaitCount} and {@link #pollFor} poll {@code client.pollEgress()}
 * throughout the wait per {@link ClusterIngressClient#pollEgress()}'s own "must be polled
 * regularly, even between offers" contract — a real gap versus a plain {@code Thread.sleep} loop,
 * fixed here, but polling alone does not resolve the stall above.
 */
class IngressIdempotencyIT {

    private static final int MEMBER_COUNT = 3;
    private static final String CREDENTIAL = InProcessCluster.SOURCE_CREDENTIAL;
    /** MarketDataDelta — the only templateId this suite's crafted messages use (see offerMessages). */
    private static final int INGRESS_TEMPLATE_ID = 9;

    @Test
    void republishingAnOverlappingTailConsumesNoSequenceIds() throws Exception {
        Map<Integer, RecordingEgressPublisher> egressByMember = new HashMap<>();
        EgressPublisher[] publishers = new EgressPublisher[MEMBER_COUNT];
        for (int i = 0; i < MEMBER_COUNT; i++) {
            RecordingEgressPublisher recorder = new RecordingEgressPublisher();
            egressByMember.put(i, recorder);
            publishers[i] = recorder;
        }

        String baseDataDir = System.getProperty("java.io.tmpdir") + "/gcm-md-it-idempotency-" + System.nanoTime();
        try (InProcessCluster cluster = InProcessCluster.start(MEMBER_COUNT, baseDataDir,
                memberId -> publishers[memberId], new EngineListener() {
        })) {
            int leader = awaitLeader(egressByMember);

            try (ClusterIngressClient client = TestIngressClients.connect(MEMBER_COUNT, CREDENTIAL)) {
                offerMessages(client, 1, 300);
                awaitCount(client, egressByMember.get(leader), 300);

                // The client never saw the acks for the tail of its previous batch, so it
                // republishes an overlapping window before resuming with genuinely new messages.
                offerMessages(client, 250, 300);
                offerMessages(client, 301, 350);
                awaitCount(client, egressByMember.get(leader), 350);

                // Give any wrongly-stamped duplicate a chance to show up before asserting its absence.
                pollFor(client, 1_000L);
            }

            // Filtered to this suite's own templateId (9): the cluster also emits heartbeats
            // (templateId 4) on its own schedule, sharing the same global sequenceId counter —
            // mixing them into a fixed-count assertion was a real bug caught by actually running
            // this suite. See RecordingEgressPublisher's class Javadoc.
            List<Long> published = egressByMember.get(leader).sequenceIdsForTemplate(INGRESS_TEMPLATE_ID);
            assertThat(published).as("no duplicate sourceSeqNum consumed a sequenceId").hasSize(350);

            Set<Long> unique = new HashSet<>(published);
            assertThat(unique).as("no duplicate sequenceIds").hasSameSizeAs(published);

            long previous = 0L;
            for (long sequenceId : published) {
                assertThat(sequenceId).as("strictly increasing (heartbeats may occupy IDs in between, so not "
                        + "necessarily contiguous integers)").isGreaterThan(previous);
                previous = sequenceId;
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

    /**
     * Blocks for {@code millis}, polling {@code client}'s egress throughout — see
     * {@link ClusterIngressClient#pollEgress()}'s Javadoc: the client's cluster session is only
     * kept alive by regular polling, including between offers. A plain {@code Thread.sleep} here
     * lets the session's keep-alive lapse. Doesn't fix the flow-control stall documented on the
     * class itself, but is a real contract gap on its own worth closing regardless.
     */
    private static void pollFor(ClusterIngressClient client, long millis) throws InterruptedException {
        long deadline = System.currentTimeMillis() + millis;
        while (System.currentTimeMillis() < deadline) {
            client.pollEgress();
            Thread.sleep(20L);
        }
    }

    private static void awaitCount(ClusterIngressClient client, RecordingEgressPublisher recorder, int expectedSize)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + 30_000L;
        while (System.currentTimeMillis() < deadline) {
            client.pollEgress();
            if (recorder.sequenceIdsForTemplate(INGRESS_TEMPLATE_ID).size() >= expectedSize) {
                return;
            }
            Thread.sleep(20L);
        }
        throw new IllegalStateException("Did not observe " + expectedSize + " published sequenceIds within timeout");
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
