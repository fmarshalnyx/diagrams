package gcm.md.sequencer.cluster;

/**
 * Answers "what sequenceId did the egress recording last show as published?" (design §6.4 point
 * 2) — the input {@link SuppressionGate#onRoleChange} needs on assuming leadership. Isolated
 * behind an interface so {@link AeronEgressPublisher} doesn't hardcode one Archive query
 * strategy, and so tests can supply a canned answer without a live Archive.
 */
interface RecordingTailQuery {

    /** Returns the last published sequenceId, or {@link SuppressionGate#NO_SUPPRESSION} if there is no prior recording. */
    long lastPublishedSequenceId();
}
