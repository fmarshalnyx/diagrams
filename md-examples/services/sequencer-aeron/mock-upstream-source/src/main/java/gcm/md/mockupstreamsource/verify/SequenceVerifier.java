package gcm.md.mockupstreamsource.verify;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * Contiguity/duplicate check on observed final-egress {@code sequenceId}s — ported near-verbatim
 * from {@code sequencer-loadgen}'s nested {@code SequenceVerifier}. As a persistent service, this
 * runs continuously rather than as a one-shot pass/fail: {@code MockUpstreamSourceMetrics}
 * exposes {@link #gapCount()}/{@link #duplicateCount()} as gauges (scraped, not polled) — the
 * same "should be permanently zero, nonzero is a real bug" contract {@code bridge_gap_total}
 * already has elsewhere in this reactor. Not a hot-path concern — this is a test tool — so a
 * boxed {@code Long} set is fine.
 */
public final class SequenceVerifier {

    private final Set<Long> seen = ConcurrentHashMap.newKeySet();
    private final AtomicLong min = new AtomicLong(Long.MAX_VALUE);
    private final AtomicLong max = new AtomicLong(Long.MIN_VALUE);
    private final LongAdder duplicates = new LongAdder();

    public void record(long sequenceId) {
        if (!seen.add(sequenceId)) {
            duplicates.increment();
        }
        min.updateAndGet(current -> Math.min(current, sequenceId));
        max.updateAndGet(current -> Math.max(current, sequenceId));
    }

    public long duplicateCount() {
        return duplicates.sum();
    }

    /** Positive iff some sequenceId between the observed min and max was never seen at all. */
    public long gapCount() {
        if (seen.isEmpty()) {
            return 0;
        }
        return (max.get() - min.get() + 1) - seen.size();
    }

    public boolean hasViolations() {
        return duplicateCount() > 0 || gapCount() > 0;
    }
}
