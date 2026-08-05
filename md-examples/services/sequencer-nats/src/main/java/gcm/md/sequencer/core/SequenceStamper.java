package gcm.md.sequencer.core;

import gcm.md.sequencer.config.SequencerProperties;
import gcm.md.sequencer.metrics.SequencerMetrics;
import org.agrona.collections.Long2LongHashMap;
import org.agrona.concurrent.OffsetEpochNanoClock;
import org.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteOrder;
import java.util.HashSet;
import java.util.Set;

/**
 * Hot-path stamping logic: writes {@code sequenceId} and {@code sequenceTimestamp} into a raw
 * SBE-encoded message at fixed byte offsets, without ever decoding the message body. All offsets
 * and template rules are compiled out of {@link SequencerProperties} once at construction into
 * plain primitives — nothing here re-reads the properties bean per message.
 *
 * <p>Not thread-safe by design: exactly one thread (the stamping/pipeline thread) may call
 * {@link #stamp(UnsafeBuffer, long)}, per the single-writer requirement in the project spec.
 */
public final class SequenceStamper {

    private static final ByteOrder LE = ByteOrder.LITTLE_ENDIAN;
    private static final long NO_EVENT = -1L;

    private final int sequenceIdOffset;
    private final int sequenceTimestampOffset;
    private final int schemaId;
    private final int schemaIdOffset;
    private final int templateIdOffset;
    private final boolean validateSchemaIdPerMessage;
    private final boolean v3Profile;
    private final Set<Integer> v3TimestampTemplateIds;

    private final boolean eventEnrichmentEnabled;
    private final int eventIdOffset;
    private final int boundaryTemplateId;
    private final int firstSequenceIdOffset;
    private final int lastSequenceIdOffset;
    private final int maxTrackedEvents;
    private final Long2LongHashMap eventFirstSeq = new Long2LongHashMap(NO_EVENT);
    private final Long2LongHashMap eventLastSeq = new Long2LongHashMap(NO_EVENT);

    private final OffsetEpochNanoClock clock;
    private final SequencerMetrics metrics;

    private boolean firstMessageSeen = false;

    /** Compiles the {@code sequencer.stamping} config tree into primitives for hot-path use. */
    public SequenceStamper(SequencerProperties properties, OffsetEpochNanoClock clock, SequencerMetrics metrics) {
        SequencerProperties.Stamping stamping = properties.getStamping();
        this.sequenceIdOffset = stamping.getSequenceIdOffset();
        this.sequenceTimestampOffset = stamping.getSequenceTimestampOffset();
        this.schemaId = stamping.getSchemaId();
        this.schemaIdOffset = stamping.getSchemaIdOffset();
        this.templateIdOffset = stamping.getTemplateIdOffset();
        this.validateSchemaIdPerMessage = stamping.isValidateSchemaIdPerMessage();
        this.v3Profile = "v3".equalsIgnoreCase(stamping.getProfile());
        this.v3TimestampTemplateIds = new HashSet<>(stamping.getTimestampTemplateIds());

        SequencerProperties.EventEnrichment enrichment = stamping.getEventEnrichment();
        this.eventEnrichmentEnabled = enrichment.isEnabled();
        this.eventIdOffset = enrichment.getEventIdOffset();
        this.boundaryTemplateId = enrichment.getBoundaryTemplateId();
        this.firstSequenceIdOffset = enrichment.getFirstSequenceIdOffset();
        this.lastSequenceIdOffset = enrichment.getLastSequenceIdOffset();
        this.maxTrackedEvents = enrichment.getMaxTrackedEvents();

        this.clock = clock;
        this.metrics = metrics;
    }

    /**
     * Stamps {@code sequenceId} (always) and {@code sequenceTimestamp} (per profile rules) into
     * {@code buffer}, plus MatchEventBoundary first/lastSequenceId enrichment when enabled.
     *
     * @return {@code true} if the message passed the schemaId sanity guard and was stamped;
     *         {@code false} if it must be dropped (schemaId mismatch)
     */
    public boolean stamp(UnsafeBuffer buffer, long sequenceId) {
        int schemaIdValue = buffer.getShort(schemaIdOffset, LE) & 0xFFFF;
        boolean mustValidate = validateSchemaIdPerMessage || !firstMessageSeen;
        firstMessageSeen = true;
        if (mustValidate && schemaIdValue != schemaId) {
            metrics.incrementSchemaMismatch();
            return false;
        }

        int templateId = buffer.getShort(templateIdOffset, LE) & 0xFFFF;

        buffer.putLong(sequenceIdOffset, sequenceId, LE);
        long sequenceTimestamp = clock.nanoTime();
        if (!v3Profile || v3TimestampTemplateIds.contains(templateId)) {
            buffer.putLong(sequenceTimestampOffset, sequenceTimestamp, LE);
        }

        if (eventEnrichmentEnabled) {
            enrichMatchEvent(buffer, templateId, sequenceId);
        }
        return true;
    }

    private void enrichMatchEvent(UnsafeBuffer buffer, int templateId, long sequenceId) {
        long eventId = buffer.getLong(eventIdOffset, LE);
        if (eventId == 0) {
            return;
        }
        if (templateId == boundaryTemplateId) {
            long first = eventFirstSeq.remove(eventId);
            long last = eventLastSeq.remove(eventId);
            // Cap exceeded or event never sighted (e.g. rollout/restart mid-event): fall back to
            // this boundary's own sequenceId rather than writing a sentinel into the wire format.
            buffer.putLong(firstSequenceIdOffset, first == NO_EVENT ? sequenceId : first, LE);
            buffer.putLong(lastSequenceIdOffset, last == NO_EVENT ? sequenceId : last, LE);
            return;
        }
        boolean alreadyTracked = eventFirstSeq.get(eventId) != NO_EVENT;
        if (!alreadyTracked && eventFirstSeq.size() >= maxTrackedEvents) {
            metrics.incrementEventTrackingEvicted();
            return;
        }
        if (!alreadyTracked) {
            eventFirstSeq.put(eventId, sequenceId);
        }
        eventLastSeq.put(eventId, sequenceId);
    }
}
