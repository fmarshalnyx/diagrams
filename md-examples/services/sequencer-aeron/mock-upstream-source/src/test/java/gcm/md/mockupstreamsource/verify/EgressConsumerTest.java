package gcm.md.mockupstreamsource.verify;

import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

import java.nio.ByteOrder;

import static org.assertj.core.api.Assertions.assertThat;

class EgressConsumerTest {

    private static final ByteOrder LE = ByteOrder.LITTLE_ENDIAN;
    private static final int MESSAGE_BATCH_TEMPLATE_ID = 10;
    private static final int OTHER_TEMPLATE_ID = 9;

    @Test
    void unbatchedModeRecordsTheSequenceIdAtOffsetEight() {
        SequenceVerifier verifier = new SequenceVerifier();
        EgressConsumer consumer = new EgressConsumer(false, verifier);

        UnsafeBuffer buffer = new UnsafeBuffer(new byte[64]);
        buffer.putShort(2, (short) OTHER_TEMPLATE_ID, LE);
        buffer.putLong(8, 555L, LE);

        consumer.onMessage(buffer);

        assertThat(consumer.observedCount()).isEqualTo(1);
        assertThat(verifier.duplicateCount()).isZero();
    }

    @Test
    void batchedModeWalksEveryEntryAndRecordsEachSequenceId() {
        SequenceVerifier verifier = new SequenceVerifier();
        EgressConsumer consumer = new EgressConsumer(true, verifier);

        UnsafeBuffer buffer = buildBatchEnvelope(new long[] {100L, 101L, 102L});

        consumer.onMessage(buffer);

        assertThat(consumer.observedCount()).isEqualTo(3);
        assertThat(verifier.gapCount()).isZero(); // 100, 101, 102 — fully contiguous
        assertThat(verifier.hasViolations()).isFalse();
    }

    @Test
    void batchedFlagFalseTreatsABatchEnvelopeAsOneOpaqueMessageInstead() {
        SequenceVerifier verifier = new SequenceVerifier();
        EgressConsumer consumer = new EgressConsumer(false, verifier);

        UnsafeBuffer buffer = buildBatchEnvelope(new long[] {100L, 101L});
        consumer.onMessage(buffer);

        assertThat(consumer.observedCount()).isEqualTo(1);
    }

    /** Hand-built to match EgressConsumer's documented offsets: templateId@2, count@35, entries from 37. */
    private static UnsafeBuffer buildBatchEnvelope(long[] sequenceIds) {
        byte[] data = new byte[64 + sequenceIds.length * 32];
        UnsafeBuffer buffer = new UnsafeBuffer(data);
        buffer.putShort(2, (short) MESSAGE_BATCH_TEMPLATE_ID, LE);
        buffer.putShort(35, (short) sequenceIds.length, LE);

        int cursor = 37;
        int blobLength = 16;
        for (long sequenceId : sequenceIds) {
            buffer.putShort(cursor, (short) blobLength, LE);
            buffer.putLong(cursor + 2 + 8, sequenceId, LE); // blob's own abs offset 8 = sequenceId
            cursor += 2 + blobLength;
        }
        return buffer;
    }
}
