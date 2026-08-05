package gcm.md.sequencer.integration.tools;

import gcm.md.sequencer.stamping.StampingConfig;

import java.util.HashMap;
import java.util.Map;

/**
 * Design §12.4's join engine: two independent NATS subscriptions (see {@link ParallelRunDiffCli})
 * feed this via {@link #onPhase1Message}/{@link #onPhase2Message} as messages arrive off
 * {@code MD_SEQUENCED} (phase-1) and whatever subject phase-2's shadow output is currently
 * targeting; this class does the join-by-{@code sourceSeqNum} and diff, buffering whichever side
 * arrives first for each key until its counterpart shows up.
 *
 * <p>Not thread-confined: NATS message callbacks for the two subscriptions run on separate
 * dispatcher threads, so every method here is synchronized. Diff-harness throughput
 * (dev-environment validation runs, not the hot path) makes that an acceptable trade for
 * correctness over raw speed.
 */
final class ParallelRunDiffHarness {

    private final MessageDiffer differ;
    private final DiffReport report = new DiffReport();

    private final Map<Long, CapturedMessage> pendingPhase1 = new HashMap<>();
    private final Map<Long, CapturedMessage> pendingPhase2 = new HashMap<>();

    ParallelRunDiffHarness(StampingConfig stampingConfig) {
        this.differ = new MessageDiffer(stampingConfig);
    }

    synchronized void onPhase1Message(CapturedMessage message) {
        CapturedMessage phase2Match = pendingPhase2.remove(message.sourceSeqNum());
        if (phase2Match == null) {
            pendingPhase1.put(message.sourceSeqNum(), message);
            return;
        }
        diffAndRecord(message.sourceSeqNum(), message, phase2Match);
    }

    synchronized void onPhase2Message(CapturedMessage message) {
        CapturedMessage phase1Match = pendingPhase1.remove(message.sourceSeqNum());
        if (phase1Match == null) {
            pendingPhase2.put(message.sourceSeqNum(), message);
            return;
        }
        diffAndRecord(message.sourceSeqNum(), phase1Match, message);
    }

    private void diffAndRecord(long sourceSeqNum, CapturedMessage phase1, CapturedMessage phase2) {
        differ.diff(phase1.payload(), phase2.payload())
                .ifPresentOrElse(
                        description -> report.recordDifferent(sourceSeqNum, description),
                        () -> report.recordEqual(sourceSeqNum));
    }

    /** Call once no more messages are expected; anything still unmatched is a real gap, not a race. */
    synchronized DiffReport finish() {
        pendingPhase1.keySet().forEach(report::recordPhase1Only);
        pendingPhase2.keySet().forEach(report::recordPhase2Only);
        return report;
    }
}
