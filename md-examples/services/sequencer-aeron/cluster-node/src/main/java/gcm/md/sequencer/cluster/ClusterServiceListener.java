package gcm.md.sequencer.cluster;

import io.aeron.cluster.service.Cluster;

/**
 * Reports {@link SequencerClusteredService} lifecycle events through callbacks so it never
 * touches a metrics library directly (design §17; same "listener indirection" pattern as
 * {@link gcm.md.sequencer.stamping.EngineListener} / {@link EgressListener}). All methods default
 * to a no-op. Called only from the clustered service's single thread — implementations must be
 * allocation-free and must not block.
 *
 * <p>Lives in {@code cluster-node}, not {@code libs/sequencer-core}, because {@link Cluster.Role}
 * is an Aeron Cluster type and {@code sequencer-core} must stay Aeron-free (Milestone 1.1).
 */
public interface ClusterServiceListener {

    /** This member's Raft role changed (design §17: {@code sequencer_cluster_role}). */
    default void onRoleChange(Cluster.Role newRole) {
    }

    /** A snapshot was written, taking {@code durationNanos} (design §17: {@code sequencer_snapshot_duration_seconds}). */
    default void onSnapshotTaken(long durationNanos) {
    }

    /** This member's replicated log position, sampled periodically (design §17: {@code sequencer_commit_position}). */
    default void onCommitPositionSample(long position) {
    }
}
