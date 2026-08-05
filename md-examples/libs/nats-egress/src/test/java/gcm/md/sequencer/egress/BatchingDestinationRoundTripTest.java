package gcm.md.sequencer.egress;

import com.usb.gcm.md.sbe.MessageBatchDecoder;
import com.usb.gcm.md.sbe.MessageHeaderDecoder;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.OffsetEpochNanoClock;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * MessageBatch round-trip (project spec §15): batch N stamped messages through
 * {@link BatchingDestination}, decode the envelope with the generated codec, verify order,
 * first/last sequenceId, and blob integrity byte-for-byte.
 */
class BatchingDestinationRoundTripTest {

    private static final ByteOrder LE = ByteOrder.LITTLE_ENDIAN;
    private static final int SCHEMA_ID = 100;
    private static final int SEQUENCE_ID_OFFSET = 8;
    private static final int TEMPLATE_ID_OFFSET = 2;
    private static final int BOUNDARY_TEMPLATE_ID = 6;

    /** Captures every buffer handed to {@link #publish}, copying out since buffers are reused. */
    private static final class CapturingDestination implements DestinationChannel {
        final List<byte[]> published = new ArrayList<>();

        @Override
        public void publish(DirectBuffer buffer, int offset, int length) {
            byte[] copy = new byte[length];
            buffer.getBytes(offset, copy, 0, length);
            published.add(copy);
        }

        @Override
        public void flush() {
        }

        @Override
        public void stop() {
        }
    }

    /** No-op: this test only cares about the wire format, not what gets counted. */
    private static final class NoOpEgressMetrics implements EgressMetrics {
        @Override
        public void incrementDropped() {
        }

        @Override
        public void incrementPublishFailures() {
        }

        @Override
        public void recordBackpressureStall(long nanos) {
        }

        @Override
        public void setInflightWindow(int inflight) {
        }

        @Override
        public void onBatchFlushed(int messageCount) {
        }
    }

    private byte[] rawMessage(int templateId, long sequenceId, int payloadByte, int payloadLength) {
        int total = 8 + 48 + payloadLength; // messageHeader + gcmHeader + payload
        UnsafeBuffer buffer = new UnsafeBuffer(new byte[total]);
        buffer.putShort(TEMPLATE_ID_OFFSET, (short) templateId, LE);
        buffer.putShort(4, (short) SCHEMA_ID, LE);
        buffer.putLong(SEQUENCE_ID_OFFSET, sequenceId, LE);
        for (int i = 0; i < payloadLength; i++) {
            buffer.putByte(56 + i, (byte) payloadByte);
        }
        byte[] out = new byte[total];
        buffer.getBytes(0, out);
        return out;
    }

    @Test
    void packsMessagesInOrderWithCorrectFirstLastSeqAndByteForByteBlobs() {
        CapturingDestination capturing = new CapturingDestination();
        BatchingConfig config = new BatchingConfig(SCHEMA_ID, SEQUENCE_ID_OFFSET, TEMPLATE_ID_OFFSET,
                BOUNDARY_TEMPLATE_ID, false, 3, 65536, 60_000_000L); // linger effectively disabled

        BatchingDestination batching = new BatchingDestination(capturing, config, new NoOpEgressMetrics(),
                new OffsetEpochNanoClock());

        byte[] m1 = rawMessage(9, 100L, 0xAA, 20);
        byte[] m2 = rawMessage(9, 101L, 0xBB, 30);
        byte[] m3 = rawMessage(9, 102L, 0xCC, 10);

        for (byte[] m : List.of(m1, m2, m3)) {
            UnsafeBuffer wrapped = new UnsafeBuffer(m);
            batching.publish(wrapped, 0, m.length);
        }
        // maxMessages=3 forces an automatic flush on the third publish.
        assertThat(capturing.published).hasSize(1);

        byte[] envelope = capturing.published.get(0);
        UnsafeBuffer envelopeBuffer = new UnsafeBuffer(envelope);
        MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();
        MessageBatchDecoder decoder = new MessageBatchDecoder();
        decoder.wrapAndApplyHeader(envelopeBuffer, 0, headerDecoder);

        assertThat(decoder.firstSequenceId()).isEqualTo(100L);
        assertThat(decoder.lastSequenceId()).isEqualTo(102L);

        List<byte[]> decodedBlobs = new ArrayList<>();
        for (MessageBatchDecoder.MessagesDecoder entry : decoder.messages()) {
            byte[] blob = new byte[entry.messageBlobLength()];
            entry.getMessageBlob(blob, 0, blob.length);
            decodedBlobs.add(blob);
        }

        assertThat(decodedBlobs).hasSize(3);
        assertThat(decodedBlobs.get(0)).isEqualTo(m1);
        assertThat(decodedBlobs.get(1)).isEqualTo(m2);
        assertThat(decodedBlobs.get(2)).isEqualTo(m3);
    }

    @Test
    void flushesImmediatelyOnAMatchEventBoundaryRegardlessOfBatchSizeThresholds() {
        CapturingDestination capturing = new CapturingDestination();
        BatchingConfig config = new BatchingConfig(SCHEMA_ID, SEQUENCE_ID_OFFSET, TEMPLATE_ID_OFFSET,
                BOUNDARY_TEMPLATE_ID, true, 100, 65536, 60_000_000L);

        BatchingDestination batching = new BatchingDestination(capturing, config, new NoOpEgressMetrics(),
                new OffsetEpochNanoClock());

        byte[] data = rawMessage(9, 1L, 0x11, 10);
        byte[] boundary = rawMessage(BOUNDARY_TEMPLATE_ID, 2L, 0x22, 10);

        batching.publish(new UnsafeBuffer(data), 0, data.length);
        assertThat(capturing.published).isEmpty();
        batching.publish(new UnsafeBuffer(boundary), 0, boundary.length);
        assertThat(capturing.published).hasSize(1);

        UnsafeBuffer envelopeBuffer = new UnsafeBuffer(capturing.published.get(0));
        MessageBatchDecoder decoder = new MessageBatchDecoder();
        decoder.wrapAndApplyHeader(envelopeBuffer, 0, new MessageHeaderDecoder());
        assertThat(decoder.flags() & 0x1).isEqualTo(1); // bit0: batch ends on an event boundary
    }
}
