package gcm.md.sequencer.stamping;

import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * MatchEventBoundary first/lastSequenceId enrichment (design §4, ported from phase-1 §8/§15)
 * under interleaving: two sources' events arrive with their data messages interspersed in the
 * global stream, since tracking is keyed per-eventId, not "messages since last boundary".
 * Source tracking is disabled here so the fixtures don't need well-formed sourceSeqNum sequences
 * — this test is isolating enrichment only.
 */
class MatchEventEnrichmentTest {

    private static final ByteOrder LE = ByteOrder.LITTLE_ENDIAN;
    private static final int DATA_TEMPLATE_ID = 1; // any non-boundary template
    private static final int BOUNDARY_TEMPLATE_ID = 6; // MatchEventBoundary, per default config

    private StampingEngine newEngine() {
        return new StampingEngine(StampingConfig.v4Defaults().withSourceTrackingDisabled(), new EngineListener() {
        });
    }

    private UnsafeBuffer message(int templateId, long eventId) {
        UnsafeBuffer buffer = new UnsafeBuffer(ByteBuffer.allocateDirect(96));
        buffer.putShort(2, (short) templateId, LE);
        buffer.putShort(4, (short) 100, LE);
        buffer.putLong(40, eventId, LE); // eventIdOffset
        return buffer;
    }

    @Test
    void firstAndLastSequenceIdAreTrackedIndependentlyPerEventIdWhenInterleaved() {
        StampingEngine engine = newEngine();

        // Global arrival order: eventA msg1, eventB msg1, eventA msg2, eventB boundary, eventA boundary.
        UnsafeBuffer eventAMsg1 = message(DATA_TEMPLATE_ID, 1001L);
        UnsafeBuffer eventBMsg1 = message(DATA_TEMPLATE_ID, 2002L);
        UnsafeBuffer eventAMsg2 = message(DATA_TEMPLATE_ID, 1001L);
        UnsafeBuffer eventBBoundary = message(BOUNDARY_TEMPLATE_ID, 2002L);
        UnsafeBuffer eventABoundary = message(BOUNDARY_TEMPLATE_ID, 1001L);

        engine.onMessage(eventAMsg1, 0, eventAMsg1.capacity(), 0L, 1L);   // seq 1
        engine.onMessage(eventBMsg1, 0, eventBMsg1.capacity(), 0L, 1L);   // seq 2
        engine.onMessage(eventAMsg2, 0, eventAMsg2.capacity(), 0L, 1L);   // seq 3
        engine.onMessage(eventBBoundary, 0, eventBBoundary.capacity(), 0L, 1L); // seq 4
        engine.onMessage(eventABoundary, 0, eventABoundary.capacity(), 0L, 1L); // seq 5

        // eventB: only one data message (seq 2) before its boundary.
        assertThat(eventBBoundary.getLong(56, LE)).isEqualTo(2L); // firstSequenceIdOffset
        assertThat(eventBBoundary.getLong(64, LE)).isEqualTo(2L); // lastSequenceIdOffset

        // eventA: two data messages (seq 1, 3) before its boundary, despite eventB interleaving.
        assertThat(eventABoundary.getLong(56, LE)).isEqualTo(1L);
        assertThat(eventABoundary.getLong(64, LE)).isEqualTo(3L);
    }

    @Test
    void trackingEntryIsRemovedOnceTheBoundaryIsProcessed() {
        StampingEngine engine = newEngine();
        UnsafeBuffer msg1 = message(DATA_TEMPLATE_ID, 5L);
        UnsafeBuffer boundary = message(BOUNDARY_TEMPLATE_ID, 5L);
        // A stray later boundary reusing the same (now-removed) eventId.
        UnsafeBuffer laterBoundaryWithStaleEventId = message(BOUNDARY_TEMPLATE_ID, 5L);

        engine.onMessage(msg1, 0, msg1.capacity(), 0L, 1L);       // seq 1
        engine.onMessage(boundary, 0, boundary.capacity(), 0L, 1L); // seq 2
        assertThat(boundary.getLong(56, LE)).isEqualTo(1L);
        assertThat(boundary.getLong(64, LE)).isEqualTo(1L);

        // eventId 5 was removed after its boundary; a stray later boundary with the same id
        // (should not normally happen, but must degrade safely) falls back to its own seq.
        engine.onMessage(laterBoundaryWithStaleEventId, 0, laterBoundaryWithStaleEventId.capacity(), 0L, 1L); // seq 3
        assertThat(laterBoundaryWithStaleEventId.getLong(56, LE)).isEqualTo(3L);
        assertThat(laterBoundaryWithStaleEventId.getLong(64, LE)).isEqualTo(3L);
    }
}
