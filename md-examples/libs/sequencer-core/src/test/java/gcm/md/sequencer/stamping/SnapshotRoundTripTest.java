package gcm.md.sequencer.stamping;

import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayDeque;
import java.util.Deque;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Snapshot round-trip and determinism (design §4, §12.3). {@code SnapshotSink}/{@code
 * SnapshotSource} don't have a real backing store yet — that lands with the Aeron cluster
 * snapshot adapter in Milestone 2 (design §5.1 {@code onTakeSnapshot}) — so this test proves the
 * contract against a trivial in-memory FIFO implementation of both interfaces.
 */
class SnapshotRoundTripTest {

    private static final ByteOrder LE = ByteOrder.LITTLE_ENDIAN;

    /** Records every put in order and replays them back in the same order. */
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
    }

    private UnsafeBuffer dataMessage(int templateId, long eventId, long sourceSeqNum) {
        UnsafeBuffer buffer = new UnsafeBuffer(ByteBuffer.allocateDirect(96));
        buffer.putShort(2, (short) templateId, LE);
        buffer.putShort(4, (short) 100, LE);
        buffer.putLong(40, eventId, LE);
        buffer.putInt(64, (int) sourceSeqNum, LE); // default tracked offset for templateId 9
        return buffer;
    }

    private UnsafeBuffer boundary(long eventId) {
        UnsafeBuffer buffer = new UnsafeBuffer(ByteBuffer.allocateDirect(96));
        buffer.putShort(2, (short) 6, LE);
        buffer.putShort(4, (short) 100, LE);
        buffer.putLong(40, eventId, LE);
        return buffer;
    }

    @Test
    void restoredEngineContinuesTheSequenceCounterMonotonically() {
        StampingEngine original = new StampingEngine(StampingConfig.v4Defaults(), new EngineListener() {
        });
        for (long seqNum = 1; seqNum <= 3; seqNum++) {
            UnsafeBuffer buf = dataMessage(9, 0, seqNum);
            original.onMessage(buf, 0, buf.capacity(), 0L, 1L);
        }
        assertThat(original.currentSequenceId()).isEqualTo(3L);

        InMemorySnapshot snapshot = new InMemorySnapshot();
        original.writeSnapshot(snapshot);

        StampingEngine restored = new StampingEngine(StampingConfig.v4Defaults(), new EngineListener() {
        });
        restored.loadSnapshot(snapshot);
        assertThat(restored.currentSequenceId()).isEqualTo(3L);

        UnsafeBuffer next = dataMessage(9, 0, 4);
        restored.onMessage(next, 0, next.capacity(), 0L, 1L);
        assertThat(restored.currentSequenceId()).isEqualTo(4L);
        assertThat(next.getLong(8, LE)).isEqualTo(4L); // continues, not restarts at 1
    }

    @Test
    void restoredEngineRepeatsSourceDedupeAndEventEnrichmentIdentically() {
        StampingEngine original = new StampingEngine(StampingConfig.v4Defaults(), new EngineListener() {
        });
        UnsafeBuffer msg1 = dataMessage(9, 777L, 1); // seq 1, eventId 777
        original.onMessage(msg1, 0, msg1.capacity(), 0L, 1L);
        UnsafeBuffer msg2 = dataMessage(9, 777L, 2); // seq 2, same event
        original.onMessage(msg2, 0, msg2.capacity(), 0L, 1L);

        InMemorySnapshot snapshot = new InMemorySnapshot();
        original.writeSnapshot(snapshot);

        StampingEngine restored = new StampingEngine(StampingConfig.v4Defaults(), new EngineListener() {
        });
        restored.loadSnapshot(snapshot);

        // A duplicate of the last pre-snapshot sourceSeqNum must still be recognized as a duplicate.
        UnsafeBuffer replay = dataMessage(9, 0, 2);
        assertThat(restored.onMessage(replay, 0, replay.capacity(), 0L, 1L)).isEqualTo(Verdict.DUPLICATE);

        // The boundary for eventId 777 must still resolve first/lastSequenceId from before the snapshot.
        UnsafeBuffer eventBoundary = boundary(777L);
        restored.onMessage(eventBoundary, 0, eventBoundary.capacity(), 0L, 1L);
        assertThat(eventBoundary.getLong(56, LE)).isEqualTo(1L);
        assertThat(eventBoundary.getLong(64, LE)).isEqualTo(2L);
    }

    @Test
    void snapshotBytesAreWrittenInSortedKeyOrderRegardlessOfInsertionOrder() {
        StampingEngine engine = new StampingEngine(StampingConfig.v4Defaults(), new EngineListener() {
        });
        // Insert source and event state out of numeric order.
        UnsafeBuffer s3 = dataMessage(9, 0, 1);
        engine.onMessage(s3, 0, s3.capacity(), 0L, 30L);
        UnsafeBuffer s1 = dataMessage(9, 0, 1);
        engine.onMessage(s1, 0, s1.capacity(), 0L, 10L);
        UnsafeBuffer s2 = dataMessage(9, 0, 1);
        engine.onMessage(s2, 0, s2.capacity(), 0L, 20L);

        InMemorySnapshot snapshot = new InMemorySnapshot();
        engine.writeSnapshot(snapshot);

        // sequenceCounter, heartbeatMessageCount, eventCount(0), sourceCount(3), then sorted (sourceId, lastSeq) pairs.
        assertThat(snapshot.nextLong()).isEqualTo(3L); // sequenceCounter
        assertThat(snapshot.nextLong()).isZero();      // heartbeatMessageCount
        assertThat(snapshot.nextInt()).isZero();       // eventCount
        assertThat(snapshot.nextInt()).isEqualTo(3);    // sourceCount
        assertThat(snapshot.nextLong()).isEqualTo(10L); // sorted ascending, not insertion order (30, 10, 20)
        assertThat(snapshot.nextLong()).isEqualTo(1L);
        assertThat(snapshot.nextLong()).isEqualTo(20L);
        assertThat(snapshot.nextLong()).isEqualTo(1L);
        assertThat(snapshot.nextLong()).isEqualTo(30L);
        assertThat(snapshot.nextLong()).isEqualTo(1L);
    }
}
