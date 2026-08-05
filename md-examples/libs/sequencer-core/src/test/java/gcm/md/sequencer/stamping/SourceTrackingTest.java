package gcm.md.sequencer.stamping;

import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Per-source ingress invariant (design §4, new in phase 2 — promotes phase-1's redelivery
 * contract into the engine itself, which is what "simplifies the line handlers"): a republished
 * {@code sourceSeqNum} is a no-op duplicate (no sequenceId consumed), a skipped one is a gap
 * (stamped normally, upstream loss is counted), and the normal case just advances.
 */
class SourceTrackingTest {

    private static final ByteOrder LE = ByteOrder.LITTLE_ENDIAN;
    private static final int TRACKED_TEMPLATE_ID = 9; // default config: templateId 9 tracked at abs offset 64
    private static final int SOURCE_SEQ_NUM_OFFSET = 64;
    private static final long SOURCE_ID = 42L;

    private UnsafeBuffer message(long sourceSeqNum) {
        UnsafeBuffer buffer = new UnsafeBuffer(ByteBuffer.allocateDirect(96));
        buffer.putShort(2, (short) TRACKED_TEMPLATE_ID, LE);
        buffer.putShort(4, (short) 100, LE);
        buffer.putInt(SOURCE_SEQ_NUM_OFFSET, (int) sourceSeqNum, LE);
        return buffer;
    }

    private static final class RecordingListener implements EngineListener {
        final List<Long> duplicates = new ArrayList<>();
        final List<long[]> gaps = new ArrayList<>(); // {sourceId, gapSize}

        @Override
        public void onSourceDuplicate(long sourceId) {
            duplicates.add(sourceId);
        }

        @Override
        public void onSourceSeqGap(long sourceId, long gapSize) {
            gaps.add(new long[] {sourceId, gapSize});
        }
    }

    @Test
    void firstMessageFromASourceIsAlwaysNormal() {
        StampingEngine engine = new StampingEngine(StampingConfig.v4Defaults(), new EngineListener() {
        });
        UnsafeBuffer first = message(1);
        assertThat(engine.onMessage(first, 0, first.capacity(), 0L, SOURCE_ID)).isEqualTo(Verdict.STAMPED);
        assertThat(engine.currentSequenceId()).isEqualTo(1L);
    }

    @Test
    void sequentialSourceSeqNumsAdvanceNormally() {
        StampingEngine engine = new StampingEngine(StampingConfig.v4Defaults(), new EngineListener() {
        });
        for (long sourceSeqNum = 1; sourceSeqNum <= 5; sourceSeqNum++) {
            UnsafeBuffer buf = message(sourceSeqNum);
            assertThat(engine.onMessage(buf, 0, buf.capacity(), 0L, SOURCE_ID)).isEqualTo(Verdict.STAMPED);
        }
        assertThat(engine.currentSequenceId()).isEqualTo(5L);
    }

    @Test
    void repeatedOrOlderSourceSeqNumIsADuplicateThatConsumesNoSequenceId() {
        RecordingListener listener = new RecordingListener();
        StampingEngine engine = new StampingEngine(StampingConfig.v4Defaults(), listener);

        UnsafeBuffer first = message(1);
        engine.onMessage(first, 0, first.capacity(), 0L, SOURCE_ID); // seq 1
        UnsafeBuffer second = message(2);
        engine.onMessage(second, 0, second.capacity(), 0L, SOURCE_ID); // seq 2

        UnsafeBuffer replay = message(2); // republish of an already-seen sourceSeqNum
        Verdict verdict = engine.onMessage(replay, 0, replay.capacity(), 0L, SOURCE_ID);

        assertThat(verdict).isEqualTo(Verdict.DUPLICATE);
        assertThat(engine.currentSequenceId()).isEqualTo(2L); // no sequenceId consumed
        assertThat(replay.getLong(8, LE)).isZero(); // not stamped
        assertThat(listener.duplicates).containsExactly(SOURCE_ID);
    }

    @Test
    void skippedSourceSeqNumIsAGapButStillStampedNormally() {
        RecordingListener listener = new RecordingListener();
        StampingEngine engine = new StampingEngine(StampingConfig.v4Defaults(), listener);

        UnsafeBuffer first = message(1);
        engine.onMessage(first, 0, first.capacity(), 0L, SOURCE_ID); // seq 1

        UnsafeBuffer gapped = message(5); // jumped from 1 to 5: 3 messages lost upstream
        Verdict verdict = engine.onMessage(gapped, 0, gapped.capacity(), 0L, SOURCE_ID);

        assertThat(verdict).isEqualTo(Verdict.STAMPED);
        assertThat(engine.currentSequenceId()).isEqualTo(2L); // still consumed a sequenceId
        assertThat(gapped.getLong(8, LE)).isEqualTo(2L);
        assertThat(listener.gaps).containsExactly(new long[] {SOURCE_ID, 3L});
    }

    @Test
    void distinctSourcesAreTrackedIndependently() {
        StampingEngine engine = new StampingEngine(StampingConfig.v4Defaults(), new EngineListener() {
        });
        UnsafeBuffer sourceAFirst = message(1);
        engine.onMessage(sourceAFirst, 0, sourceAFirst.capacity(), 0L, 1L);
        // Source B's first message reuses sourceSeqNum=1 — must not be treated as a duplicate of source A.
        UnsafeBuffer sourceBFirst = message(1);
        assertThat(engine.onMessage(sourceBFirst, 0, sourceBFirst.capacity(), 0L, 2L)).isEqualTo(Verdict.STAMPED);
    }

    @Test
    void untrackedTemplateIdsBypassSourceTrackingEntirely() {
        StampingEngine engine = new StampingEngine(StampingConfig.v4Defaults(), new EngineListener() {
        });
        UnsafeBuffer buffer = new UnsafeBuffer(ByteBuffer.allocateDirect(64));
        buffer.putShort(2, (short) 1, LE); // templateId 1 is not in the default tracking map
        buffer.putShort(4, (short) 100, LE);
        // No sourceSeqNum field set at all — would be garbage if read, proving it's never touched.
        assertThat(engine.onMessage(buffer, 0, buffer.capacity(), 0L, SOURCE_ID)).isEqualTo(Verdict.STAMPED);
        assertThat(engine.onMessage(buffer, 0, buffer.capacity(), 0L, SOURCE_ID)).isEqualTo(Verdict.STAMPED);
    }
}
