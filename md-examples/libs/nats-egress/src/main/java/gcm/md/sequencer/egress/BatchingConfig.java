package gcm.md.sequencer.egress;

/**
 * {@code MessageBatch} (templateId=10) framing config for {@link BatchingDestination}: the
 * stamping offsets needed to read a message's sequenceId/templateId back out, plus the
 * batching thresholds themselves (project spec §8).
 */
public record BatchingConfig(int schemaId, int sequenceIdOffset, int templateIdOffset, int boundaryTemplateId,
                              boolean flushOnEventBoundary, int maxMessages, int maxBytes, long maxLingerMicros) {
}
