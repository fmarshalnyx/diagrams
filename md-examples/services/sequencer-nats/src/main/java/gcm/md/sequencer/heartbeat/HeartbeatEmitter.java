package gcm.md.sequencer.heartbeat;

import gcm.md.sequencer.config.SequencerProperties;
import org.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;

/**
 * Builds the sequencer's own {@code Heartbeat} (templateId=4) message as raw bytes (project spec
 * §8). {@code sequenceId}/{@code sequenceTimestamp} are left as zero placeholders — the message
 * is fed through the exact same {@link gcm.md.sequencer.core.SequenceStamper} path as any
 * ingress message, so it consumes a sequenceId like any other and makes gap detection sound in
 * quiet markets.
 *
 * <p>Hand-rolled with raw {@link UnsafeBuffer} writes rather than the generated SBE codec, for
 * the same reason as {@link gcm.md.sequencer.egress.BatchingDestination}: codecs are test-only.
 * Timing is driven externally by {@link gcm.md.sequencer.core.SequencerPipeline}; this class only
 * knows how to lay out one message.
 */
public final class HeartbeatEmitter {

    private static final ByteOrder LE = ByteOrder.LITTLE_ENDIAN;

    // Heartbeat (schema id=100, v4): gcmHeader(48) + highWaterSequenceId(8) + messageCount(8)
    // + instrumentCount(4) + heartbeatType(1) + source(8) = 77.
    private static final int ROOT_BLOCK_LENGTH = 77;
    private static final int HEARTBEAT_TEMPLATE_ID = 4;
    private static final int SCHEMA_VERSION = 4;
    private static final int MESSAGE_LENGTH = 8 + ROOT_BLOCK_LENGTH;
    private static final int HEARTBEAT_TYPE_SEQUENCER = 1;
    private static final int SOURCE_FIELD_LENGTH = 8;

    private static final int HIGH_WATER_SEQUENCE_ID_OFFSET = 56;
    private static final int MESSAGE_COUNT_OFFSET = 64;
    private static final int INSTRUMENT_COUNT_OFFSET = 72;
    private static final int HEARTBEAT_TYPE_OFFSET = 76;
    private static final int SOURCE_OFFSET = 77;

    private final boolean enabled;
    private final long intervalMillis;
    private final byte[] sourceId;
    private final int schemaId;
    private final int schemaIdOffset;
    private final int templateIdOffset;

    private long messageCount;

    /** Compiles the {@code sequencer.heartbeat} config into primitives. */
    public HeartbeatEmitter(SequencerProperties properties) {
        SequencerProperties.Heartbeat config = properties.getHeartbeat();
        this.enabled = config.isEnabled();
        this.intervalMillis = config.getIntervalMs();
        this.sourceId = paddedAscii(config.getSourceId(), SOURCE_FIELD_LENGTH);
        this.schemaId = properties.getStamping().getSchemaId();
        this.schemaIdOffset = properties.getStamping().getSchemaIdOffset();
        this.templateIdOffset = properties.getStamping().getTemplateIdOffset();
    }

    /** Returns whether sequencer-emitted heartbeats are enabled. */
    public boolean isEnabled() {
        return enabled;
    }

    /** Returns the configured emission interval in milliseconds. */
    public long getIntervalMillis() {
        return intervalMillis;
    }

    /**
     * Builds one Heartbeat message carrying {@code highWaterSequenceId}. Must only be called from
     * the single hot-path writer (see {@link gcm.md.sequencer.core.SequencerPipeline}), since it
     * mutates the cumulative {@code messageCount} field.
     */
    public byte[] buildMessage(long highWaterSequenceId) {
        messageCount++;
        byte[] data = new byte[MESSAGE_LENGTH];
        UnsafeBuffer buffer = new UnsafeBuffer(data);
        buffer.putShort(0, (short) ROOT_BLOCK_LENGTH, LE);
        buffer.putShort(templateIdOffset, (short) HEARTBEAT_TEMPLATE_ID, LE);
        buffer.putShort(schemaIdOffset, (short) schemaId, LE);
        buffer.putShort(6, (short) SCHEMA_VERSION, LE);
        // gcmHeader (abs 8..55): sequenceId/sequenceTimestamp are stamper-owned placeholders;
        // sourceTimestamp/ingestTimestamp/eventId/reserved1 are correctly zero for a sequencer heartbeat.
        buffer.putLong(HIGH_WATER_SEQUENCE_ID_OFFSET, highWaterSequenceId, LE);
        buffer.putLong(MESSAGE_COUNT_OFFSET, messageCount, LE);
        buffer.putInt(INSTRUMENT_COUNT_OFFSET, 0, LE);
        buffer.putByte(HEARTBEAT_TYPE_OFFSET, (byte) HEARTBEAT_TYPE_SEQUENCER);
        buffer.putBytes(SOURCE_OFFSET, sourceId);
        return data;
    }

    private static byte[] paddedAscii(String value, int length) {
        byte[] padded = new byte[length];
        byte[] ascii = value.getBytes(StandardCharsets.US_ASCII);
        System.arraycopy(ascii, 0, padded, 0, Math.min(ascii.length, length));
        return padded;
    }
}
