package gcm.md.natsbridge.bridge;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Design §9: phase-2 egress is contiguous by construction, so this pure decision logic gets
 * exhaustive coverage — {@code bridge_gap_total} going nonzero should never happen, but the
 * tracker's own gap-detection arithmetic must still be provably correct.
 */
class ContiguityTrackerTest {

    @Test
    void freshTrackerBridgesTheFirstMessageNormally() {
        ContiguityTracker tracker = new ContiguityTracker(0L);
        ContiguityTracker.Evaluation result = tracker.evaluate(1L);
        assertThat(result.decision()).isEqualTo(ContiguityTracker.Decision.NORMAL_BRIDGE);
        assertThat(result.gapSize()).isZero();
        assertThat(tracker.lastBridged()).isEqualTo(1L);
    }

    @Test
    void sequentialMessagesAreAllNormal() {
        ContiguityTracker tracker = new ContiguityTracker(0L);
        for (long seq = 1; seq <= 5; seq++) {
            assertThat(tracker.evaluate(seq).decision()).isEqualTo(ContiguityTracker.Decision.NORMAL_BRIDGE);
        }
        assertThat(tracker.lastBridged()).isEqualTo(5L);
    }

    @Test
    void sequenceIdAtOrBeforeTheCheckpointIsSkipped() {
        ContiguityTracker tracker = new ContiguityTracker(100L); // resumed from a checkpoint
        assertThat(tracker.evaluate(50L).decision()).isEqualTo(ContiguityTracker.Decision.SKIP_ALREADY_BRIDGED);
        assertThat(tracker.evaluate(100L).decision()).isEqualTo(ContiguityTracker.Decision.SKIP_ALREADY_BRIDGED);
        assertThat(tracker.lastBridged()).isEqualTo(100L); // unchanged by skipped evaluations
    }

    @Test
    void resumesNormallyJustPastTheCheckpoint() {
        ContiguityTracker tracker = new ContiguityTracker(100L);
        ContiguityTracker.Evaluation result = tracker.evaluate(101L);
        assertThat(result.decision()).isEqualTo(ContiguityTracker.Decision.NORMAL_BRIDGE);
        assertThat(tracker.lastBridged()).isEqualTo(101L);
    }

    @Test
    void aSkippedSequenceIdIsFlaggedAsAGapWithTheCorrectSize() {
        ContiguityTracker tracker = new ContiguityTracker(0L);
        tracker.evaluate(1L);
        ContiguityTracker.Evaluation result = tracker.evaluate(5L); // 2,3,4 missing
        assertThat(result.decision()).isEqualTo(ContiguityTracker.Decision.GAP_BUT_BRIDGE);
        assertThat(result.gapSize()).isEqualTo(3L);
        assertThat(tracker.lastBridged()).isEqualTo(5L); // still advances despite the gap
    }

    @Test
    void replayOverlapWithLiveStreamIsSkippedNotDoubleBridged() {
        ContiguityTracker tracker = new ContiguityTracker(0L);
        tracker.evaluate(1L);
        tracker.evaluate(2L);
        tracker.evaluate(3L);

        // The live stream re-delivers 2 and 3 during the replay/live merge overlap window.
        assertThat(tracker.evaluate(2L).decision()).isEqualTo(ContiguityTracker.Decision.SKIP_ALREADY_BRIDGED);
        assertThat(tracker.evaluate(3L).decision()).isEqualTo(ContiguityTracker.Decision.SKIP_ALREADY_BRIDGED);
        assertThat(tracker.evaluate(4L).decision()).isEqualTo(ContiguityTracker.Decision.NORMAL_BRIDGE);
    }

    @Test
    void aBriefSkipRunBelowThresholdDoesNotResetTheCheckpoint() {
        ContiguityTracker tracker = new ContiguityTracker(100L, 5);
        for (long seq = 1; seq <= 4; seq++) {
            assertThat(tracker.evaluate(seq).decision()).isEqualTo(ContiguityTracker.Decision.SKIP_ALREADY_BRIDGED);
        }
        assertThat(tracker.lastBridged()).isEqualTo(100L); // still unchanged, threshold not reached
    }

    @Test
    void aSustainedSkipRunAtTheThresholdRebasesTheCheckpoint() {
        ContiguityTracker tracker = new ContiguityTracker(100L, 5);
        for (long seq = 1; seq <= 4; seq++) {
            tracker.evaluate(seq); // 4 consecutive skips, still below threshold
        }
        ContiguityTracker.Evaluation result = tracker.evaluate(5L); // 5th consecutive skip - trips it
        assertThat(result.decision()).isEqualTo(ContiguityTracker.Decision.CHECKPOINT_RESET_AND_BRIDGE);
        assertThat(tracker.lastBridged()).isEqualTo(5L); // rebased to the triggering sequenceId

        // Bridging resumes normally past the new baseline.
        assertThat(tracker.evaluate(6L).decision()).isEqualTo(ContiguityTracker.Decision.NORMAL_BRIDGE);
    }

    @Test
    void aNormalBridgeInTheMiddleResetsTheConsecutiveSkipCounter() {
        ContiguityTracker tracker = new ContiguityTracker(100L, 3);
        tracker.evaluate(1L); // skip 1/3
        tracker.evaluate(2L); // skip 2/3
        tracker.evaluate(101L); // NORMAL_BRIDGE - counter resets
        // Two more skips shouldn't trip a threshold of 3, since the counter was reset above.
        assertThat(tracker.evaluate(3L).decision()).isEqualTo(ContiguityTracker.Decision.SKIP_ALREADY_BRIDGED);
        assertThat(tracker.evaluate(4L).decision()).isEqualTo(ContiguityTracker.Decision.SKIP_ALREADY_BRIDGED);
        assertThat(tracker.lastBridged()).isEqualTo(101L);
    }

    @Test
    void singleArgConstructorDisablesResetRecoveryEvenForALongSkipRun() {
        ContiguityTracker tracker = new ContiguityTracker(1_000_000L);
        for (long seq = 1; seq <= 10_000; seq++) {
            assertThat(tracker.evaluate(seq).decision()).isEqualTo(ContiguityTracker.Decision.SKIP_ALREADY_BRIDGED);
        }
        assertThat(tracker.lastBridged()).isEqualTo(1_000_000L);
    }
}
