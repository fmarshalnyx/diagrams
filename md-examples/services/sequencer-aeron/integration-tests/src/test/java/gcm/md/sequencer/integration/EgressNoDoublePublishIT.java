package gcm.md.sequencer.integration;

import gcm.md.sequencer.cluster.AeronEgressConfig;
import gcm.md.sequencer.cluster.AeronEgressPublisher;
import gcm.md.sequencer.cluster.EgressListener;
import gcm.md.sequencer.cluster.EgressPublisher;
import gcm.md.sequencer.clusterclient.ClusterIngressClient;
import gcm.md.sequencer.stamping.EngineListener;
import io.aeron.Aeron;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.cluster.service.Cluster;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.LockSupport;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Design §6.4 / §12.2: publishing is a side effect; log replay after restart and re-processing
 * after failover must never re-emit an already-published sequenceId. Unlike the other tests in
 * this suite, this one deliberately uses the real {@link AeronEgressPublisher} (not
 * {@link RecordingEgressPublisher}) for every member — {@link SuppressionGate}'s gate logic only
 * runs inside the real implementation, so a stand-in wouldn't actually exercise the invariant
 * under test. A raw Aeron subscription plays the role nats-bridge would in production, observing
 * the real egress wire output across the whole run including a leader restart.
 *
 * <p>{@code @Disabled} — see this module's pom.xml confidence note.
 */
class EgressNoDoublePublishIT {

    private static final int MEMBER_COUNT = 3;
    private static final String CREDENTIAL = InProcessCluster.SOURCE_CREDENTIAL;
    private static final String EGRESS_CHANNEL = "aeron:udp?control=localhost:19070|control-mode=dynamic";
    private static final int EGRESS_STREAM_ID = 1;

    @Test
    void noSequenceIdIsEverPublishedTwiceAcrossFailoverAndRestart() throws Exception {
        String baseDataDir = System.getProperty("java.io.tmpdir") + "/gcm-md-it-nodoublepub-" + System.nanoTime();

        MediaDriver.Context observerDriverCtx = new MediaDriver.Context()
                .threadingMode(ThreadingMode.SHARED).dirDeleteOnStart(true).dirDeleteOnShutdown(true);
        MediaDriver observerDriver = MediaDriver.launchEmbedded(observerDriverCtx);
        Aeron observerAeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(observerDriver.aeronDirectoryName()));
        Subscription observerSubscription = observerAeron.addSubscription(EGRESS_CHANNEL, EGRESS_STREAM_ID);

        List<Long> observedSequenceIds = new ArrayList<>();
        Map<Integer, RoleObservingEgressPublisher> publishersByMember = new HashMap<>();
        try (InProcessCluster cluster = InProcessCluster.start(MEMBER_COUNT, baseDataDir, memberId -> {
            RoleObservingEgressPublisher publisher = new RoleObservingEgressPublisher(
                    newRealEgressPublisher(memberId));
            publishersByMember.put(memberId, publisher);
            return publisher;
        }, new EngineListener() {
        })) {
            try (ClusterIngressClient client = TestIngressClients.connect(MEMBER_COUNT, CREDENTIAL)) {
                offerAndDrain(client, 1, 300, observerSubscription, observedSequenceIds);

                // Simulate a leader restart mid-stream — the exact scenario the suppression gate
                // exists to protect: on reassuming leadership, the new/restarted leader must
                // query the recorded tail and resume only past it, never re-emitting.
                int currentLeader = awaitLeader(publishersByMember);
                cluster.killMember(currentLeader);
                cluster.restartMember(currentLeader);

                offerAndDrain(client, 301, 600, observerSubscription, observedSequenceIds);
            }
        } finally {
            observerAeron.close();
            observerDriver.close();
        }

        Set<Long> unique = new HashSet<>(observedSequenceIds);
        assertThat(unique).as("no sequenceId observed twice on egress").hasSameSizeAs(observedSequenceIds);
    }

    private static EgressPublisher newRealEgressPublisher(int memberId) {
        AeronEgressConfig egressConfig = new AeronEgressConfig(EGRESS_CHANNEL, EGRESS_STREAM_ID, 8, 1_000, 1_000_000L, 500_000_000L);
        AeronArchive.Context archiveClientCtx = new AeronArchive.Context()
                .controlRequestChannel("aeron:udp?endpoint=localhost:" + (9010 + memberId * 100 + 40))
                .controlResponseChannel("aeron:udp?endpoint=localhost:" + (9010 + memberId * 100 + 41));
        return new AeronEgressPublisher(egressConfig, archiveClientCtx, new EgressListener() {
        });
    }

    /** Delegates everything to a real {@link EgressPublisher} but also exposes the latest role, since the real implementation has no such introspection hook. */
    private static final class RoleObservingEgressPublisher implements EgressPublisher {
        private final EgressPublisher delegate;
        private volatile Cluster.Role role = Cluster.Role.FOLLOWER;

        RoleObservingEgressPublisher(EgressPublisher delegate) {
            this.delegate = delegate;
        }

        @Override
        public void onStart(Cluster cluster) {
            delegate.onStart(cluster);
        }

        @Override
        public void publish(DirectBuffer buffer, int offset, int length, long sequenceId) {
            delegate.publish(buffer, offset, length, sequenceId);
        }

        @Override
        public void onRoleChange(Cluster.Role newRole) {
            role = newRole;
            delegate.onRoleChange(newRole);
        }

        @Override
        public void onTerminate() {
            delegate.onTerminate();
        }

        boolean isLeader() {
            return role == Cluster.Role.LEADER;
        }
    }

    private static int awaitLeader(Map<Integer, RoleObservingEgressPublisher> publishersByMember) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 30_000L;
        while (System.currentTimeMillis() < deadline) {
            for (Map.Entry<Integer, RoleObservingEgressPublisher> entry : publishersByMember.entrySet()) {
                if (entry.getValue().isLeader()) {
                    return entry.getKey();
                }
            }
            Thread.sleep(100L);
        }
        throw new IllegalStateException("No leader elected within timeout");
    }

    private static void offerAndDrain(ClusterIngressClient client, long fromSourceSeqNum, long toSourceSeqNum,
                                       Subscription subscription, List<Long> observedSequenceIds) {
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

            subscription.poll((fragment, offset, length, header) ->
                    observedSequenceIds.add(fragment.getLong(offset + 8, ByteOrder.LITTLE_ENDIAN)), 10);
        }
    }
}
