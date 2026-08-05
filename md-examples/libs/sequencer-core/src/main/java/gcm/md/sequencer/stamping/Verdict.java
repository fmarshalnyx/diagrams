package gcm.md.sequencer.stamping;

/** Outcome of {@link StampingEngine#onMessage} (design §4). */
public enum Verdict {

    /** Stamped and assigned a sequenceId; the host should publish it. */
    STAMPED,

    /** A source republished an already-seen {@code sourceSeqNum}; not stamped, no sequenceId consumed. */
    DUPLICATE,

    /** Failed the schemaId sanity guard; not stamped, no sequenceId consumed. */
    REJECTED_SCHEMA
}
