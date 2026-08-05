package gcm.md.sequencer.integration.tools;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;

/**
 * Accumulated result of one §12.4 parallel-run: every {@code sourceSeqNum} that matched
 * byte-for-byte (outside the masked fields), every one that matched but differed, and every one
 * seen on only one side (a gap in one phase's pipeline — always a bug, never expected).
 */
final class DiffReport {

    private final List<Long> matchedEqual = new ArrayList<>();
    private final List<Mismatch> matchedDifferent = new ArrayList<>();
    private final TreeSet<Long> phase1Only = new TreeSet<>();
    private final TreeSet<Long> phase2Only = new TreeSet<>();

    record Mismatch(long sourceSeqNum, String description) {
    }

    void recordEqual(long sourceSeqNum) {
        matchedEqual.add(sourceSeqNum);
    }

    void recordDifferent(long sourceSeqNum, String description) {
        matchedDifferent.add(new Mismatch(sourceSeqNum, description));
    }

    void recordPhase1Only(long sourceSeqNum) {
        phase1Only.add(sourceSeqNum);
    }

    void recordPhase2Only(long sourceSeqNum) {
        phase2Only.add(sourceSeqNum);
    }

    /** {@code false} means at least one thing this run exists to catch actually happened. */
    boolean isClean() {
        return matchedDifferent.isEmpty() && phase1Only.isEmpty() && phase2Only.isEmpty();
    }

    String toSummaryText() {
        StringBuilder sb = new StringBuilder();
        sb.append("Parallel-run diff report (design §12.4) — ").append(Instant.now()).append('\n');
        sb.append("matched, byte-identical outside sequenceId/sequenceTimestamp: ").append(matchedEqual.size()).append('\n');
        sb.append("matched, but differed elsewhere: ").append(matchedDifferent.size()).append('\n');
        sb.append("phase-1-only (missing from phase-2 output): ").append(phase1Only.size())
                .append(phase1Only.isEmpty() ? "" : " " + phase1Only).append('\n');
        sb.append("phase-2-only (missing from phase-1 output): ").append(phase2Only.size())
                .append(phase2Only.isEmpty() ? "" : " " + phase2Only).append('\n');
        if (!matchedDifferent.isEmpty()) {
            sb.append("\nMismatches:\n");
            for (Mismatch mismatch : matchedDifferent) {
                sb.append("  sourceSeqNum=").append(mismatch.sourceSeqNum())
                        .append(": ").append(mismatch.description()).append('\n');
            }
        }
        sb.append('\n').append(isClean() ? "RESULT: CLEAN" : "RESULT: FIDELITY REGRESSION DETECTED").append('\n');
        return sb.toString();
    }
}
