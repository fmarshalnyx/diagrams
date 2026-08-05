package gcm.md.natsbridge.bridge;

/**
 * Decides what to do with each consumed sequenceId (design §9): skip it if it was already
 * bridged (the replay-merge overlaps with the live stream, or a restart re-replays from the
 * checkpoint), otherwise bridge it — flagging a gap if it isn't the very next expected one.
 * Pure logic, independent of Aeron/NATS, so it's fully unit-testable.
 *
 * <p>Design §2.4/§9: phase-2 egress is contiguous by construction (unlike phase-1). A
 * {@link Decision#GAP_BUT_BRIDGE} outcome should therefore never happen in a correctly
 * functioning system — {@code bridge_gap_total} going nonzero is an egress bug, not routine
 * noise.
 *
 * <p><b>Checkpoint-reset recovery:</b> a genuinely production-safe multi-member cluster preserves
 * sequenceId continuity across leadership changes via Raft, so the checkpoint should never end up
 * ahead of what the live stream can ever produce again. It was observed doing exactly that in
 * local testing, though: a single-member `cluster-node` losing its non-persisted (`emptyDir`)
 * Archive/consensus state on pod recreation restarts the sequenceId space from a low value while
 * the bridge's own checkpoint stays at the old high-water mark, which without recovery makes
 * {@link #evaluate} return {@link Decision#SKIP_ALREADY_BRIDGED} forever — {@code bridge_messages}
 * goes permanently static with no error, recoverable previously only by manually restarting
 * nats-bridge and/or clearing its NATS-KV checkpoint. After {@code resetThresholdMessages}
 * *consecutive* skip decisions, the checkpoint is assumed stale and rebased to the current
 * sequenceId, reported as {@link Decision#CHECKPOINT_RESET_AND_BRIDGE} so the caller can emit a
 * dedicated, always-should-be-zero metric distinct from {@code bridge_gap_total} — this is a
 * heuristic, not a proof: if a legitimate replay-catchup backlog since the last checkpoint write
 * ever exceeds the threshold, this will rebase prematurely and cause a duplicate publish of
 * already-bridged messages. Callers should size the threshold well above their expected
 * worst-case backlog (bounded by Archive recording retention, not by
 * {@code checkpointIntervalMessages}) for their deployment.
 */
final class ContiguityTracker {

    /** Effectively disables checkpoint-reset recovery — used where only skip/gap logic is under test. */
    static final int RESET_DISABLED = Integer.MAX_VALUE;

    enum Decision {
        /** Already bridged (at or before the checkpoint/replay-live overlap) — do not republish. */
        SKIP_ALREADY_BRIDGED,
        /** Bridged, but it wasn't the immediately-next expected sequenceId. */
        GAP_BUT_BRIDGE,
        /** Bridged, and it was exactly the next expected sequenceId. */
        NORMAL_BRIDGE,
        /**
         * A sustained run of below-checkpoint sequenceIds was seen ({@code resetThresholdMessages}
         * consecutive {@link #SKIP_ALREADY_BRIDGED} decisions) — the checkpoint is assumed stale
         * and has been rebased to this sequenceId, which is bridged as the new baseline.
         */
        CHECKPOINT_RESET_AND_BRIDGE
    }

    record Evaluation(Decision decision, long gapSize) {
    }

    private final int resetThresholdMessages;
    private long lastBridged;
    private long consecutiveSkips;

    ContiguityTracker(long checkpoint) {
        this(checkpoint, RESET_DISABLED);
    }

    ContiguityTracker(long checkpoint, int resetThresholdMessages) {
        this.lastBridged = checkpoint;
        this.resetThresholdMessages = resetThresholdMessages;
    }

    Evaluation evaluate(long sequenceId) {
        if (sequenceId <= lastBridged) {
            consecutiveSkips++;
            if (consecutiveSkips >= resetThresholdMessages) {
                consecutiveSkips = 0;
                lastBridged = sequenceId;
                return new Evaluation(Decision.CHECKPOINT_RESET_AND_BRIDGE, 0L);
            }
            return new Evaluation(Decision.SKIP_ALREADY_BRIDGED, 0L);
        }
        consecutiveSkips = 0;
        long gapSize = sequenceId - lastBridged - 1;
        lastBridged = sequenceId;
        return new Evaluation(gapSize > 0 ? Decision.GAP_BUT_BRIDGE : Decision.NORMAL_BRIDGE, gapSize);
    }

    long lastBridged() {
        return lastBridged;
    }
}
