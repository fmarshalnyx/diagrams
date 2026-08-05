package gcm.md.sequencer.cluster;

import io.aeron.cluster.service.Cluster;

/**
 * The replay/failover suppression gate (design §6.4) — the load-bearing invariant that log
 * replay after restart and re-processing after failover never re-emit an already-published
 * message. Pure decision logic, deliberately independent of Aeron/Archive plumbing so it is
 * fully unit-testable without a live cluster; {@link AeronEgressPublisher} owns the actual
 * publication and only consults this class for the publish/drop decision.
 *
 * <p>"Live (not replaying)" (design §6.4 point 1) needs no separate check here: a replica's
 * {@link Cluster.Role} only ever becomes {@code LEADER} once its own log replay/catch-up has
 * finished, so tracking role alone already captures both conditions.
 */
final class SuppressionGate {

    /** Sentinel meaning "no prior recording / nothing to suppress" (design §6.4: "empty recording → snapshot floor"). */
    static final long NO_SUPPRESSION = -1L;

    private Cluster.Role role = Cluster.Role.FOLLOWER;
    private long suppressUpTo = NO_SUPPRESSION;

    /**
     * Called on {@code onRoleChange}. {@code lastPublishedSequenceId} is only meaningful when
     * {@code newRole == LEADER}; pass {@link #NO_SUPPRESSION} for an empty/absent prior recording.
     */
    void onRoleChange(Cluster.Role newRole, long lastPublishedSequenceId) {
        this.role = newRole;
        this.suppressUpTo = newRole == Cluster.Role.LEADER ? lastPublishedSequenceId : NO_SUPPRESSION;
    }

    /**
     * Returns whether the message assigned {@code sequenceId} must be dropped: either this
     * replica isn't the live leader, or it's still catching up to what a prior leader's
     * recording already shows as published. Once {@code sequenceId} passes {@code suppressUpTo}
     * this returns {@code false} for every subsequent (larger) sequenceId without needing an
     * explicit "resume" step (design §6.4 point 3).
     */
    boolean shouldSuppress(long sequenceId) {
        if (role != Cluster.Role.LEADER) {
            return true;
        }
        return sequenceId <= suppressUpTo;
    }

    /** Returns the current suppression floor (for metrics/diagnostics only). */
    long suppressUpTo() {
        return suppressUpTo;
    }
}
