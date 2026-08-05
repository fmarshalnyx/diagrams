package gcm.md.sequencer.stamping;

import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Design §12.3, the load-bearing determinism suite: {@link StampingEngine} is a pure function of
 * (current state, input buffer, supplied time), so replaying the identical log through two
 * independent instances — or through one instance that snapshots partway and one that doesn't —
 * must produce byte-identical stamped output and byte-identical snapshots. This is what makes
 * state-machine replication (every cluster replica reaches the same result) and snapshot-based
 * catch-up both safe.
 */
class DeterminismTest {

    private static final ByteOrder LE = ByteOrder.LITTLE_ENDIAN;
    private static final long SOURCE_A = 1L;
    private static final long SOURCE_B = 2L;

    /** One canned log entry: template/eventId/sourceSeqNum/sourceId/timeNanos, replayed identically per engine. */
    private record LogEntry(int templateId, long eventId, long sourceSeqNum, long sourceId, long timeNanos) {

        UnsafeBuffer toBuffer() {
            UnsafeBuffer buffer = new UnsafeBuffer(new byte[96]);
            buffer.putShort(2, (short) templateId, LE);
            buffer.putShort(4, (short) 100, LE); // schemaId
            buffer.putLong(40, eventId, LE);
            if (templateId == 9) {
                buffer.putInt(64, (int) sourceSeqNum, LE); // default tracked offset for templateId 9
            }
            return buffer;
        }
    }

    /** A canned mixed-traffic log: two sources, interleaved data/boundary messages, a source gap. */
    private static List<LogEntry> cannedLog() {
        List<LogEntry> log = new ArrayList<>();
        log.add(new LogEntry(9, 0L, 1, SOURCE_A, 100L));
        log.add(new LogEntry(9, 777L, 2, SOURCE_A, 200L));       // event 777 opens
        log.add(new LogEntry(1, 0L, 0, SOURCE_B, 250L));          // untracked template, other source
        log.add(new LogEntry(9, 777L, 3, SOURCE_A, 300L));        // event 777 continues
        log.add(new LogEntry(6, 777L, 0, SOURCE_A, 400L));        // boundary closes event 777
        log.add(new LogEntry(9, 0L, 5, SOURCE_A, 500L));          // sourceSeqNum jumps 3->5: a gap
        log.add(new LogEntry(9, 0L, 6, SOURCE_A, 600L));
        return log;
    }

    private static final class InMemorySnapshot implements SnapshotSink, SnapshotSource {
        private final Deque<Long> longs = new ArrayDeque<>();
        private final Deque<Integer> ints = new ArrayDeque<>();

        @Override
        public void putLong(long value) {
            longs.addLast(value);
        }

        @Override
        public void putInt(int value) {
            ints.addLast(value);
        }

        @Override
        public long nextLong() {
            return longs.removeFirst();
        }

        @Override
        public int nextInt() {
            return ints.removeFirst();
        }

        /** Byte-for-byte comparison of the recorded primitive sequence, in write order. */
        boolean isByteIdenticalTo(InMemorySnapshot other) {
            return List.copyOf(longs).equals(List.copyOf(other.longs))
                    && List.copyOf(ints).equals(List.copyOf(other.ints));
        }
    }

    private static StampingEngine newEngine() {
        return new StampingEngine(StampingConfig.v4Defaults(), new EngineListener() {
        });
    }

    private static void feed(StampingEngine engine, List<LogEntry> log, List<byte[]> outputCollector) {
        for (LogEntry entry : log) {
            UnsafeBuffer buffer = entry.toBuffer();
            engine.onMessage(buffer, 0, buffer.capacity(), entry.timeNanos(), entry.sourceId());
            byte[] copy = new byte[buffer.capacity()];
            buffer.getBytes(0, copy);
            outputCollector.add(copy);
        }
    }

    @Test
    void replayEquivalence_sameLogThroughTwoFreshEnginesProducesByteIdenticalOutput() {
        List<LogEntry> log = cannedLog();

        StampingEngine engineA = newEngine();
        List<byte[]> outputA = new ArrayList<>();
        feed(engineA, log, outputA);

        StampingEngine engineB = newEngine();
        List<byte[]> outputB = new ArrayList<>();
        feed(engineB, log, outputB);

        assertThat(outputA).hasSize(log.size());
        for (int i = 0; i < outputA.size(); i++) {
            assertThat(outputA.get(i)).as("message %d stamped bytes", i).isEqualTo(outputB.get(i));
        }
        assertThat(engineA.currentSequenceId()).isEqualTo(engineB.currentSequenceId());

        InMemorySnapshot snapshotA = new InMemorySnapshot();
        InMemorySnapshot snapshotB = new InMemorySnapshot();
        engineA.writeSnapshot(snapshotA);
        engineB.writeSnapshot(snapshotB);
        assertThat(snapshotA.isByteIdenticalTo(snapshotB)).as("snapshots are byte-identical").isTrue();
    }

    @Test
    void snapshotEquivalence_snapshotMidwayThenResumeMatchesContinuousReplay() {
        List<LogEntry> log = cannedLog();
        int splitPoint = 4; // snapshot after the first 4 entries, resume with the rest

        // Path 1: continuous replay, no snapshot involved.
        StampingEngine continuousEngine = newEngine();
        List<byte[]> continuousOutput = new ArrayList<>();
        feed(continuousEngine, log, continuousOutput);

        // Path 2: replay the first half, snapshot, construct a fresh engine from that snapshot,
        // then replay the second half on the fresh engine.
        StampingEngine firstHalfEngine = newEngine();
        List<byte[]> firstHalfOutput = new ArrayList<>();
        feed(firstHalfEngine, log.subList(0, splitPoint), firstHalfOutput);

        InMemorySnapshot midSnapshot = new InMemorySnapshot();
        firstHalfEngine.writeSnapshot(midSnapshot);

        StampingEngine resumedEngine = newEngine();
        resumedEngine.loadSnapshot(midSnapshot);
        List<byte[]> resumedOutput = new ArrayList<>();
        feed(resumedEngine, log.subList(splitPoint, log.size()), resumedOutput);

        // The first half's own stamped output must match between both paths (trivially, since
        // both replayed the identical prefix from a fresh engine — this is really re-confirming
        // replay equivalence for the prefix).
        for (int i = 0; i < splitPoint; i++) {
            assertThat(continuousOutput.get(i)).as("prefix message %d", i).isEqualTo(firstHalfOutput.get(i));
        }

        // The load-bearing assertion: the SECOND half's stamped output, produced by an engine
        // that only ever saw a snapshot plus the tail, must byte-for-byte match what the
        // continuously-replayed engine produced for that same tail.
        for (int i = splitPoint; i < log.size(); i++) {
            assertThat(continuousOutput.get(i)).as("post-snapshot message %d", i)
                    .isEqualTo(resumedOutput.get(i - splitPoint));
        }

        assertThat(resumedEngine.currentSequenceId()).isEqualTo(continuousEngine.currentSequenceId());

        InMemorySnapshot continuousFinal = new InMemorySnapshot();
        continuousEngine.writeSnapshot(continuousFinal);
        InMemorySnapshot resumedFinal = new InMemorySnapshot();
        resumedEngine.writeSnapshot(resumedFinal);
        assertThat(resumedFinal.isByteIdenticalTo(continuousFinal))
                .as("final snapshot (counter, eventId map, source map) is byte-identical").isTrue();
    }
}
