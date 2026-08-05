package gcm.md.mockupstreamsource.verify;

import io.nats.client.Connection;
import io.nats.client.JetStream;
import org.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteOrder;
import java.util.concurrent.atomic.LongAdder;

/**
 * Subscribes to the sequencer's final observed egress subject and feeds a {@link SequenceVerifier}
 * — ported from {@code sequencer-loadgen}'s {@code startEgressConsumer}/{@code recordBatch}/
 * {@code recordOne}, split out so the batch-unpacking logic ({@link #onMessage}/{@link
 * #recordBatch}) is directly testable against a hand-built or codec-built envelope without a
 * live NATS connection. Runs for the lifetime of the service — there's no "verify window" to
 * close, unlike {@code sequencer-loadgen}'s one-shot run.
 *
 * <p><b>Must subscribe via JetStream, not a plain core NATS dispatcher</b> — confirmed the hard
 * way running this against a live cluster: a core subscription has no flow control, so under
 * sustained load NATS silently drops messages for a slow consumer, which showed up as {@code
 * mock_upstream_gap} growing even while {@code nats-bridge}'s own {@code bridge_gap}/{@code
 * bridge_publish_failures} stayed at zero — the messages were genuinely, successfully published,
 * this consumer just never received its own copy of some of them. An ephemeral JetStream push
 * consumer (this class's {@link #subscribe}) gets proper bounded delivery instead of a drop.
 *
 * <p><b>Trade-off, not fully resolved:</b> JetStream's at-least-once delivery means this
 * consumer's own subscription can occasionally observe a genuinely-published message twice (a
 * redelivery after a slow/lost ack, not a system-level double-publish — that invariant is
 * separately covered by {@code EgressNoDoublePublishIT}/{@code SuppressionGate}), which {@link
 * SequenceVerifier#duplicateCount()} correctly, if a little noisily, counts. The proper fix is a
 * {@code Nats-Msg-Id} header on {@code nats-bridge}'s publish (server-side dedup within the
 * stream's duplicate window), not implemented yet — see the tracked follow-up. Net trade favors
 * this over the core-NATS drop: a false-positive duplicate count from the verifier's own
 * redelivery is a far smaller, better-understood problem than silently losing gap visibility
 * entirely.
 */
public final class EgressConsumer {

    private static final ByteOrder LE = ByteOrder.LITTLE_ENDIAN;
    private static final int MESSAGE_BATCH_TEMPLATE_ID = 10;
    private static final int TEMPLATE_ID_OFFSET = 2;
    private static final int SEQUENCE_ID_OFFSET = 8;
    /** Hand-decoded, matching schema/md-models-sbe-v4.xml's MessageBatch groupSizeEncoding layout. */
    private static final int BATCH_COUNT_OFFSET = 35;
    private static final int BATCH_FIRST_ENTRY_OFFSET = 37;

    private final boolean batched;
    private final SequenceVerifier verifier;
    private final LongAdder observedTotal = new LongAdder();

    public EgressConsumer(boolean batched, SequenceVerifier verifier) {
        this.batched = batched;
        this.verifier = verifier;
    }

    /** Wires this consumer to a live subject; the actual decision logic is in {@link #onMessage}. */
    public void subscribe(Connection connection, JetStream jetStream, String subject) throws Exception {
        jetStream.subscribe(subject, connection.createDispatcher(),
                msg -> onMessage(new UnsafeBuffer(msg.getData())), true);
    }

    /** Package-visible and NATS-free so it's directly unit-testable. */
    void onMessage(UnsafeBuffer buffer) {
        int templateId = buffer.getShort(TEMPLATE_ID_OFFSET, LE) & 0xFFFF;
        if (batched && templateId == MESSAGE_BATCH_TEMPLATE_ID) {
            recordBatch(buffer);
        } else {
            recordOne(buffer, 0);
        }
    }

    void recordBatch(UnsafeBuffer envelope) {
        int count = envelope.getShort(BATCH_COUNT_OFFSET, LE) & 0xFFFF;
        int cursor = BATCH_FIRST_ENTRY_OFFSET;
        for (int i = 0; i < count; i++) {
            int blobLength = envelope.getShort(cursor, LE) & 0xFFFF;
            recordOne(envelope, cursor + 2);
            cursor += 2 + blobLength;
        }
    }

    void recordOne(UnsafeBuffer buffer, int offset) {
        verifier.record(buffer.getLong(offset + SEQUENCE_ID_OFFSET, LE));
        observedTotal.increment();
    }

    public long observedCount() {
        return observedTotal.sum();
    }
}
