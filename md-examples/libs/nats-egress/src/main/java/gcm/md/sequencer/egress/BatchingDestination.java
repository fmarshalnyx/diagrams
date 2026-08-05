package gcm.md.sequencer.egress;

import org.agrona.DirectBuffer;
import org.agrona.concurrent.OffsetEpochNanoClock;
import org.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Decorator that packs stamped messages into a schema {@code MessageBatch} (templateId=10)
 * envelope before handing them to a wrapped {@link DestinationChannel} (project spec §8).
 * JetStream's cost is per-publish, not per-byte, so batching ~100 messages per publish is what
 * makes 1M logical msgs/sec achievable on a persisted stream.
 *
 * <p>The envelope is written with raw {@link UnsafeBuffer} ops against one reusable direct
 * buffer (memcpy each message in, publish, reset) — allocation-free in steady state. This is
 * hand-rolled rather than built with the generated SBE codec because batching sits on the hot
 * path, and the project spec reserves SBE-generated codecs for tests only.
 *
 * <p>{@code MessageBatch}'s own wire layout (templateId, root block length, group encoding) is
 * fixed by the frozen v4 schema, not a per-deployment stamping choice, so — unlike the
 * sequenceId/sequenceTimestamp offsets — it is not pulled from config.
 */
public final class BatchingDestination implements DestinationChannel {

    private static final ByteOrder LE = ByteOrder.LITTLE_ENDIAN;

    // MessageBatch (schema id=100, v4): fixed root block = firstSeq(8)+lastSeq(8)+ts(8)+flags(1) = 25.
    private static final int ROOT_BLOCK_LENGTH = 25;
    private static final int MESSAGE_BATCH_TEMPLATE_ID = 10;
    private static final int SCHEMA_VERSION = 4;
    // messageHeader(8) + root block(25) + groupSizeEncoding(4) = 37: where packed message blobs start.
    private static final int ENVELOPE_HEADER_BYTES = 8 + ROOT_BLOCK_LENGTH + 4;
    private static final int MAX_SINGLE_MESSAGE_BYTES = 65534; // varBlobEncoding.length maxValue
    private static final int LENGTH_PREFIX_BYTES = 2;
    private static final byte FLAG_ENDS_ON_EVENT_BOUNDARY = 0x1;

    private final DestinationChannel delegate;
    private final EgressMetrics metrics;
    private final OffsetEpochNanoClock clock;
    private final int schemaId;
    private final int sequenceIdOffset;
    private final int templateIdOffset;
    private final int boundaryTemplateId;
    private final boolean flushOnEventBoundary;
    private final int maxMessages;
    private final int maxBytes;
    private final ScheduledExecutorService lingerScheduler;

    private final UnsafeBuffer scratch;

    private int messageCount;
    private int bodyBytesUsed;
    private long batchFirstSeq;
    private long batchLastSeq;
    private boolean endsOnBoundary;
    private volatile boolean lingerDue;

    /** Wraps {@code delegate}, packing messages per {@code sequencer.egress.batching} config. */
    public BatchingDestination(DestinationChannel delegate, BatchingConfig config, EgressMetrics metrics,
                                OffsetEpochNanoClock clock) {
        this.delegate = delegate;
        this.metrics = metrics;
        this.clock = clock;
        this.schemaId = config.schemaId();
        this.sequenceIdOffset = config.sequenceIdOffset();
        this.templateIdOffset = config.templateIdOffset();
        this.boundaryTemplateId = config.boundaryTemplateId();

        this.flushOnEventBoundary = config.flushOnEventBoundary();
        this.maxMessages = config.maxMessages();
        this.maxBytes = config.maxBytes();

        int capacity = ENVELOPE_HEADER_BYTES + maxBytes + LENGTH_PREFIX_BYTES + MAX_SINGLE_MESSAGE_BYTES;
        this.scratch = new UnsafeBuffer(ByteBuffer.allocateDirect(capacity));

        this.lingerScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "sequencer-batch-linger");
            t.setDaemon(true);
            return t;
        });
        long lingerMicros = Math.max(1, config.maxLingerMicros());
        lingerScheduler.scheduleAtFixedRate(() -> lingerDue = true, lingerMicros, lingerMicros, TimeUnit.MICROSECONDS);
    }

    @Override
    public void publish(DirectBuffer buffer, int offset, int length) {
        if (lingerDue && messageCount > 0) {
            flushLocked();
        }
        lingerDue = false;

        int needed = LENGTH_PREFIX_BYTES + length;
        if (messageCount >= maxMessages || bodyBytesUsed + needed > maxBytes) {
            flushLocked();
        }

        int writeAt = ENVELOPE_HEADER_BYTES + bodyBytesUsed;
        scratch.putShort(writeAt, (short) length, LE);
        scratch.putBytes(writeAt + LENGTH_PREFIX_BYTES, buffer, offset, length);
        bodyBytesUsed += needed;

        long seq = buffer.getLong(offset + sequenceIdOffset, LE);
        if (messageCount == 0) {
            batchFirstSeq = seq;
        }
        batchLastSeq = seq;
        messageCount++;

        boolean isBoundary = flushOnEventBoundary
                && (buffer.getShort(offset + templateIdOffset, LE) & 0xFFFF) == boundaryTemplateId;
        if (isBoundary) {
            endsOnBoundary = true;
            flushLocked();
        } else if (messageCount >= maxMessages || bodyBytesUsed >= maxBytes) {
            flushLocked();
        }
    }

    @Override
    public void flush() {
        flushLocked();
    }

    private void flushLocked() {
        if (messageCount == 0) {
            return;
        }
        scratch.putShort(0, (short) ROOT_BLOCK_LENGTH, LE);
        scratch.putShort(2, (short) MESSAGE_BATCH_TEMPLATE_ID, LE);
        scratch.putShort(4, (short) schemaId, LE);
        scratch.putShort(6, (short) SCHEMA_VERSION, LE);
        scratch.putLong(8, batchFirstSeq, LE);
        scratch.putLong(16, batchLastSeq, LE);
        scratch.putLong(24, clock.nanoTime(), LE); // batchTimestamp
        scratch.putByte(32, endsOnBoundary ? FLAG_ENDS_ON_EVENT_BOUNDARY : 0);
        scratch.putShort(33, (short) 0, LE); // group entry fixed-block length: 0 (only a var-length blob per entry)
        scratch.putShort(35, (short) messageCount, LE);

        delegate.publish(scratch, 0, ENVELOPE_HEADER_BYTES + bodyBytesUsed);
        metrics.onBatchFlushed(messageCount);

        messageCount = 0;
        bodyBytesUsed = 0;
        batchFirstSeq = 0;
        batchLastSeq = 0;
        endsOnBoundary = false;
    }

    @Override
    public void awaitInFlightDrained() {
        flushLocked();
        delegate.awaitInFlightDrained();
    }

    @Override
    public void stop() {
        lingerScheduler.shutdownNow();
        flushLocked();
        delegate.stop();
    }
}
