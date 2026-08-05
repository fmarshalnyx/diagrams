package gcm.md.sequencer.stamping;

import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Int2IntHashMap;
import org.agrona.collections.IntHashSet;
import org.agrona.collections.Long2LongHashMap;

import java.nio.ByteOrder;
import java.util.Arrays;

/**
 * The transport-agnostic stamping engine (design §4): the factored-out heart of the phase-1
 * sequencer and the body of the phase-2 clustered service. Everything here is a pure function of
 * (current state, input buffer, supplied time) — no clocks, no I/O, no threads. Exactly one
 * thread may call {@link #onMessage} / {@link #onHeartbeatTimer} at a time, per the single-writer
 * requirement; all mutable state lives in this instance and is fully captured by
 * {@link #writeSnapshot}.
 *
 * <p>Unlike phase-1's {@code SequenceStamper}, there is no block allocator: the sequence counter
 * is a plain {@code long} because the clustered log makes every assignment replicated,
 * deterministic state — gaps are no longer needed to avoid per-message persistence.
 */
public final class StampingEngine {

    private static final ByteOrder LE = ByteOrder.LITTLE_ENDIAN;
    private static final long NO_EVENT = -1L;
    private static final long NO_SOURCE_SEEN = -1L;
    private static final int NO_SOURCE_TRACKING_OFFSET = -1;

    // Heartbeat (templateId=4) wire layout, ported verbatim from phase-1's HeartbeatEmitter.
    // Structural to the schema (like MessageBatch's hand-rolled framing in libs/nats-egress), so
    // — unlike the stamping-contract offsets above — these are not part of StampingConfig.
    private static final int HEARTBEAT_TEMPLATE_ID = 4;
    private static final int HEARTBEAT_SCHEMA_VERSION = 4;
    private static final int HEARTBEAT_ROOT_BLOCK_LENGTH = 77;
    private static final int HEARTBEAT_MESSAGE_LENGTH = 8 + HEARTBEAT_ROOT_BLOCK_LENGTH;
    private static final int HEARTBEAT_TYPE_SEQUENCER = 1;
    private static final int HEARTBEAT_SOURCE_FIELD_LENGTH = 8;
    private static final int HEARTBEAT_HIGH_WATER_SEQUENCE_ID_OFFSET = 56;
    private static final int HEARTBEAT_MESSAGE_COUNT_OFFSET = 64;
    private static final int HEARTBEAT_INSTRUMENT_COUNT_OFFSET = 72;
    private static final int HEARTBEAT_TYPE_OFFSET = 76;
    private static final int HEARTBEAT_SOURCE_OFFSET = 77;

    private final StampingConfig cfg;
    private final EngineListener listener;
    private final IntHashSet v3TimestampTemplateIds;
    private final Int2IntHashMap sourceSeqNumOffsetByTemplateId = new Int2IntHashMap(NO_SOURCE_TRACKING_OFFSET);
    private final byte[] heartbeatSourceId;

    private final Long2LongHashMap eventFirstSeq = new Long2LongHashMap(NO_EVENT);
    private final Long2LongHashMap eventLastSeq = new Long2LongHashMap(NO_EVENT);
    private final Long2LongHashMap sourceLastSeqNum = new Long2LongHashMap(NO_SOURCE_SEEN);

    private long sequenceCounter;
    private long heartbeatMessageCount;
    private boolean firstMessageSeen;

    public StampingEngine(StampingConfig cfg, EngineListener listener) {
        this.cfg = cfg;
        this.listener = listener;

        this.v3TimestampTemplateIds = new IntHashSet();
        for (int templateId : cfg.v3TimestampTemplateIds()) {
            v3TimestampTemplateIds.add(templateId);
        }

        if (cfg.sourceTracking().enabled()) {
            cfg.sourceTracking().sourceSeqNumOffsetByTemplateId()
                    .forEach(sourceSeqNumOffsetByTemplateId::put);
        }

        this.heartbeatSourceId = paddedAscii(cfg.heartbeatSourceId(), HEARTBEAT_SOURCE_FIELD_LENGTH);
    }

    /**
     * Processes one ingress message in log order; may mutate {@code buf} in place.
     * {@code timeNanos} MUST be deterministic cluster time, never a local clock — the same
     * (state, buffer, timeNanos, sourceId) sequence must produce byte-identical output on every
     * replica (design §12.3).
     *
     * @param sourceId the authenticated ingress identity (design §7), never read from the payload
     */
    public Verdict onMessage(MutableDirectBuffer buf, int offset, int length, long timeNanos, long sourceId) {
        int schemaIdValue = buf.getShort(offset + cfg.schemaIdOffset(), LE) & 0xFFFF;
        boolean mustValidate = cfg.validateSchemaIdPerMessage() || !firstMessageSeen;
        firstMessageSeen = true;
        if (mustValidate && schemaIdValue != cfg.schemaId()) {
            listener.onSchemaMismatch();
            return Verdict.REJECTED_SCHEMA;
        }

        int templateId = buf.getShort(offset + cfg.templateIdOffset(), LE) & 0xFFFF;

        int sourceSeqNumOffset = sourceSeqNumOffsetByTemplateId.get(templateId);
        if (sourceSeqNumOffset != NO_SOURCE_TRACKING_OFFSET) {
            long sourceSeqNum = buf.getInt(offset + sourceSeqNumOffset, LE) & 0xFFFFFFFFL;
            long last = sourceLastSeqNum.get(sourceId);
            if (last != NO_SOURCE_SEEN && sourceSeqNum <= last) {
                listener.onSourceDuplicate(sourceId);
                return Verdict.DUPLICATE;
            }
            if (last != NO_SOURCE_SEEN && sourceSeqNum > last + 1) {
                listener.onSourceSeqGap(sourceId, sourceSeqNum - last - 1);
            }
            sourceLastSeqNum.put(sourceId, sourceSeqNum);
        }

        long sequenceId = ++sequenceCounter;
        stampSequenceFields(buf, offset, sequenceId, timeNanos, templateId);
        return Verdict.STAMPED;
    }

    /**
     * Builds and stamps one Heartbeat (templateId=4) message into {@code scratch}, consuming a
     * sequenceId exactly like any ingress message — {@code highWaterSequenceId} in the payload
     * equals that same sequenceId. Returns the message length.
     */
    public int onHeartbeatTimer(MutableDirectBuffer scratch, long timeNanos) {
        heartbeatMessageCount++;
        long sequenceId = ++sequenceCounter;

        scratch.putShort(0, (short) HEARTBEAT_ROOT_BLOCK_LENGTH, LE);
        scratch.putShort(cfg.templateIdOffset(), (short) HEARTBEAT_TEMPLATE_ID, LE);
        scratch.putShort(cfg.schemaIdOffset(), (short) cfg.schemaId(), LE);
        scratch.putShort(6, (short) HEARTBEAT_SCHEMA_VERSION, LE);
        scratch.putLong(HEARTBEAT_HIGH_WATER_SEQUENCE_ID_OFFSET, sequenceId, LE);
        scratch.putLong(HEARTBEAT_MESSAGE_COUNT_OFFSET, heartbeatMessageCount, LE);
        scratch.putInt(HEARTBEAT_INSTRUMENT_COUNT_OFFSET, 0, LE);
        scratch.putByte(HEARTBEAT_TYPE_OFFSET, (byte) HEARTBEAT_TYPE_SEQUENCER);
        scratch.putBytes(HEARTBEAT_SOURCE_OFFSET, heartbeatSourceId);

        stampSequenceFields(scratch, 0, sequenceId, timeNanos, HEARTBEAT_TEMPLATE_ID);
        return HEARTBEAT_MESSAGE_LENGTH;
    }

    /** Returns the last sequenceId assigned by {@link #onMessage} or {@link #onHeartbeatTimer} (0 if none). */
    public long currentSequenceId() {
        return sequenceCounter;
    }

    /** Writes all replicated state in a deterministic (sorted-key) order — design §12.3. */
    public void writeSnapshot(SnapshotSink sink) {
        sink.putLong(sequenceCounter);
        sink.putLong(heartbeatMessageCount);

        long[] eventIds = sortedKeys(eventFirstSeq);
        sink.putInt(eventIds.length);
        for (long eventId : eventIds) {
            sink.putLong(eventId);
            sink.putLong(eventFirstSeq.get(eventId));
            sink.putLong(eventLastSeq.get(eventId));
        }

        long[] sourceIds = sortedKeys(sourceLastSeqNum);
        sink.putInt(sourceIds.length);
        for (long sourceId : sourceIds) {
            sink.putLong(sourceId);
            sink.putLong(sourceLastSeqNum.get(sourceId));
        }
    }

    /** Replaces all replicated state with a snapshot written by {@link #writeSnapshot}. */
    public void loadSnapshot(SnapshotSource source) {
        eventFirstSeq.clear();
        eventLastSeq.clear();
        sourceLastSeqNum.clear();

        sequenceCounter = source.nextLong();
        heartbeatMessageCount = source.nextLong();

        int eventCount = source.nextInt();
        for (int i = 0; i < eventCount; i++) {
            long eventId = source.nextLong();
            long first = source.nextLong();
            long last = source.nextLong();
            eventFirstSeq.put(eventId, first);
            eventLastSeq.put(eventId, last);
        }

        int sourceCount = source.nextInt();
        for (int i = 0; i < sourceCount; i++) {
            long sourceId = source.nextLong();
            long last = source.nextLong();
            sourceLastSeqNum.put(sourceId, last);
        }
    }

    private void stampSequenceFields(MutableDirectBuffer buf, int offset, long sequenceId, long timeNanos, int templateId) {
        buf.putLong(offset + cfg.sequenceIdOffset(), sequenceId, LE);
        if (!cfg.v3Profile() || v3TimestampTemplateIds.contains(templateId)) {
            buf.putLong(offset + cfg.sequenceTimestampOffset(), timeNanos, LE);
        }
        if (cfg.eventEnrichment().enabled()) {
            enrichMatchEvent(buf, offset, templateId, sequenceId);
        }
    }

    private void enrichMatchEvent(MutableDirectBuffer buf, int offset, int templateId, long sequenceId) {
        StampingConfig.EventEnrichmentConfig enrichment = cfg.eventEnrichment();
        long eventId = buf.getLong(offset + enrichment.eventIdOffset(), LE);
        if (eventId == 0) {
            return;
        }
        if (templateId == enrichment.boundaryTemplateId()) {
            long first = eventFirstSeq.remove(eventId);
            long last = eventLastSeq.remove(eventId);
            // Cap exceeded or event never sighted (e.g. rollout/restart mid-event): fall back to
            // this boundary's own sequenceId rather than writing a sentinel into the wire format.
            buf.putLong(offset + enrichment.firstSequenceIdOffset(), first == NO_EVENT ? sequenceId : first, LE);
            buf.putLong(offset + enrichment.lastSequenceIdOffset(), last == NO_EVENT ? sequenceId : last, LE);
            return;
        }
        boolean alreadyTracked = eventFirstSeq.get(eventId) != NO_EVENT;
        if (!alreadyTracked && eventFirstSeq.size() >= enrichment.maxTrackedEvents()) {
            listener.onEventTrackingEvicted();
            return;
        }
        if (!alreadyTracked) {
            eventFirstSeq.put(eventId, sequenceId);
        }
        eventLastSeq.put(eventId, sequenceId);
    }

    private static long[] sortedKeys(Long2LongHashMap map) {
        long[] keys = new long[map.size()];
        int i = 0;
        Long2LongHashMap.KeyIterator it = map.keySet().iterator();
        while (it.hasNext()) {
            keys[i++] = it.nextValue();
        }
        Arrays.sort(keys);
        return keys;
    }

    private static byte[] paddedAscii(String value, int length) {
        byte[] padded = new byte[length];
        byte[] ascii = value.getBytes(java.nio.charset.StandardCharsets.US_ASCII);
        System.arraycopy(ascii, 0, padded, 0, Math.min(ascii.length, length));
        return padded;
    }
}
