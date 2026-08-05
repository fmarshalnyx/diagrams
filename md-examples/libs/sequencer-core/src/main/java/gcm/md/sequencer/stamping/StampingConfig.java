package gcm.md.sequencer.stamping;

import java.util.Map;
import java.util.Set;

/**
 * Compiles the {@code sequencer.stamping}/{@code sequencer.core.source-tracking} config trees
 * (design §4) into a plain, Spring-free value the {@link StampingEngine} is constructed with.
 * Hosts (phase-1's {@code SequencerProperties}, phase-2's cluster-node config) map their own
 * bound configuration onto this record, the same pattern {@code libs/nats-egress}'s
 * {@code EgressConfig}/{@code BatchingConfig} already use.
 *
 * @param v3Profile                  {@code true} for the v3 compatibility profile (sequenceTimestamp
 *                                   stamped only for {@code v3TimestampTemplateIds}, no event
 *                                   enrichment); {@code false} for v4 (unconditional stamping).
 * @param sequenceIdOffset           absolute byte offset {@code sequenceId} is written to (v4: 8).
 * @param sequenceTimestampOffset    absolute byte offset {@code sequenceTimestamp} is written to (v4: 32).
 * @param schemaId                  expected SBE {@code schemaId}, verified as a sanity guard.
 * @param schemaIdOffset             absolute byte offset of {@code schemaId} in the SBE messageHeader.
 * @param templateIdOffset           absolute byte offset of {@code templateId} in the SBE messageHeader.
 * @param validateSchemaIdPerMessage {@code false}: only the first message is checked; {@code true}: every message.
 * @param v3TimestampTemplateIds     v3 profile only — templateIds that get sequenceTimestamp stamped.
 * @param eventEnrichment            MatchEventBoundary first/lastSequenceId enrichment config.
 * @param sourceTracking             per-source ingress dedupe/gap-detection config (new in phase 2).
 * @param heartbeatSourceId          value stamped into the heartbeat's {@code source} field (max 8 ASCII chars).
 */
public record StampingConfig(
        boolean v3Profile,
        int sequenceIdOffset,
        int sequenceTimestampOffset,
        int schemaId,
        int schemaIdOffset,
        int templateIdOffset,
        boolean validateSchemaIdPerMessage,
        Set<Integer> v3TimestampTemplateIds,
        EventEnrichmentConfig eventEnrichment,
        SourceTrackingConfig sourceTracking,
        String heartbeatSourceId) {

    /** MatchEventBoundary firstSequenceId/lastSequenceId enrichment (design §4, phase-1 §8). */
    public record EventEnrichmentConfig(
            boolean enabled,
            int eventIdOffset,
            int boundaryTemplateId,
            int firstSequenceIdOffset,
            int lastSequenceIdOffset,
            int maxTrackedEvents) {

        /** v4 defaults, matching phase-1's {@code sequencer.stamping.event-enrichment.*}. */
        public static EventEnrichmentConfig defaults() {
            return new EventEnrichmentConfig(true, 40, 6, 56, 64, 65536);
        }

        public static EventEnrichmentConfig disabled() {
            return new EventEnrichmentConfig(false, 40, 6, 56, 64, 65536);
        }
    }

    /**
     * Per-source ingress invariant (design §4, new in phase 2 — promotes phase-1's redelivery
     * contract into the engine itself): a message's {@code sourceSeqNum} is read at the offset
     * configured for its templateId (only templates present in {@code sourceSeqNumOffsetByTemplateId}
     * are tracked), and compared against the last value seen for its {@code sourceId}.
     */
    public record SourceTrackingConfig(boolean enabled, Map<Integer, Integer> sourceSeqNumOffsetByTemplateId) {

        /** v4 defaults: tracking enabled for templateId 9 (MarketDataDelta), sourceSeqNum at abs offset 64. */
        public static SourceTrackingConfig defaults() {
            return new SourceTrackingConfig(true, Map.of(9, 64));
        }

        public static SourceTrackingConfig disabled() {
            return new SourceTrackingConfig(false, Map.of());
        }
    }

    /** v4 profile defaults, matching {@code md-models-sbe-v4.xml} / phase-1's {@code application.yml}. */
    public static StampingConfig v4Defaults() {
        return new StampingConfig(false, 8, 32, 100, 4, 2, false, Set.of(),
                EventEnrichmentConfig.defaults(), SourceTrackingConfig.defaults(), "SEQR");
    }

    /**
     * v3 compatibility profile: sequenceId stamped always, sequenceTimestamp only for
     * {@code timestampTemplateIds}, no event enrichment, no source tracking (phase-1 §3 v3 mode
     * predates both).
     */
    public static StampingConfig v3Profile(Set<Integer> timestampTemplateIds) {
        return new StampingConfig(true, 8, 32, 100, 4, 2, false, timestampTemplateIds,
                EventEnrichmentConfig.disabled(), SourceTrackingConfig.disabled(), "SEQR");
    }

    /** Returns a copy with source tracking disabled — useful for isolating other behavior in tests. */
    public StampingConfig withSourceTrackingDisabled() {
        return new StampingConfig(v3Profile, sequenceIdOffset, sequenceTimestampOffset, schemaId, schemaIdOffset,
                templateIdOffset, validateSchemaIdPerMessage, v3TimestampTemplateIds, eventEnrichment,
                SourceTrackingConfig.disabled(), heartbeatSourceId);
    }
}
