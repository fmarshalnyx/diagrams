package gcm.md.sequencer.integration;

import gcm.md.sequencer.cluster.EgressPublisher;
import io.aeron.cluster.service.Cluster;
import org.agrona.DirectBuffer;

import java.nio.ByteOrder;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * Test-support {@link EgressPublisher}: records every publish (with its sequenceId and
 * templateId) and the latest role, without any real Aeron egress plumbing — used by the
 * in-process suite to observe "what would have been published" and "who is currently leader" per
 * member, since production code has no other way to ask a running member either of those things
 * from outside.
 *
 * <p>Records <b>every</b> publish, including heartbeat messages (templateId 4) — the cluster
 * emits these on its own schedule independent of test-driven ingress traffic, sharing the same
 * global sequenceId counter (design §4). Callers asserting exact counts/contiguity on their own
 * ingress messages specifically must filter via {@link #sequenceIdsForTemplate}, not assume every
 * recorded entry came from their own {@code offer()} calls — a real bug caught by actually
 * running this suite (heartbeats silently inflated fixed-count assertions).
 */
final class RecordingEgressPublisher implements EgressPublisher {

    private static final int TEMPLATE_ID_OFFSET = 2;

    // ConcurrentLinkedQueue, not CopyOnWriteArrayList: publish() is called on the cluster's own
    // hot message-processing path, potentially hundreds of times per test — a copy-on-every-add
    // list turns that into O(n^2) total work and was observed to visibly stall processing under
    // load (a real bug caught by actually running this suite against a live 3-member cluster).
    private final Queue<PublishedMessage> published = new ConcurrentLinkedQueue<>();
    private final AtomicReference<Cluster.Role> role = new AtomicReference<>(Cluster.Role.FOLLOWER);

    private record PublishedMessage(long sequenceId, int templateId) {
    }

    @Override
    public void onStart(Cluster cluster) {
    }

    @Override
    public void publish(DirectBuffer buffer, int offset, int length, long sequenceId) {
        int templateId = buffer.getShort(offset + TEMPLATE_ID_OFFSET, ByteOrder.LITTLE_ENDIAN) & 0xFFFF;
        published.add(new PublishedMessage(sequenceId, templateId));
    }

    @Override
    public void onRoleChange(Cluster.Role newRole) {
        role.set(newRole);
    }

    @Override
    public void onTerminate() {
    }

    boolean isLeader() {
        return role.get() == Cluster.Role.LEADER;
    }

    /** Every published sequenceId, in publish order, including heartbeats. */
    List<Long> publishedSequenceIds() {
        return published.stream().map(PublishedMessage::sequenceId).collect(Collectors.toList());
    }

    /** Published sequenceIds restricted to one templateId, in publish order — see class Javadoc. */
    List<Long> sequenceIdsForTemplate(int templateId) {
        return published.stream()
                .filter(message -> message.templateId() == templateId)
                .map(PublishedMessage::sequenceId)
                .collect(Collectors.toList());
    }
}
