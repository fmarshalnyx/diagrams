package gcm.md.sequencer.core;

import gcm.md.sequencer.config.SequencerProperties;
import gcm.md.sequencer.metrics.SequencerMetrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.agrona.concurrent.OffsetEpochNanoClock;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * MatchEventBoundary first/lastSequenceId enrichment (project spec §8, §15) under interleaving:
 * two sources' events arrive with their data messages interspersed in the global stream, since
 * tracking is keyed per-eventId, not "messages since last boundary".
 */
class MatchEventEnrichmentTest {

    private static final ByteOrder LE = ByteOrder.LITTLE_ENDIAN;
    private static final int DATA_TEMPLATE_ID = 9; // MarketDataDelta
    private static final int BOUNDARY_TEMPLATE_ID = 6; // MatchEventBoundary, per default config

    private SequenceStamper stamper;

    private SequenceStamper newStamper() {
        SequencerProperties properties = new SequencerProperties();
        return new SequenceStamper(properties, new OffsetEpochNanoClock(), new SequencerMetrics(new SimpleMeterRegistry()));
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
        stamper = newStamper();

        // Global arrival order: eventA msg1, eventB msg1, eventA msg2, eventB boundary, eventA boundary.
        UnsafeBuffer eventAMsg1 = message(DATA_TEMPLATE_ID, 1001L);
        UnsafeBuffer eventBMsg1 = message(DATA_TEMPLATE_ID, 2002L);
        UnsafeBuffer eventAMsg2 = message(DATA_TEMPLATE_ID, 1001L);
        UnsafeBuffer eventBBoundary = message(BOUNDARY_TEMPLATE_ID, 2002L);
        UnsafeBuffer eventABoundary = message(BOUNDARY_TEMPLATE_ID, 1001L);

        stamper.stamp(eventAMsg1, 10L);
        stamper.stamp(eventBMsg1, 11L);
        stamper.stamp(eventAMsg2, 12L);
        stamper.stamp(eventBBoundary, 13L);
        stamper.stamp(eventABoundary, 14L);

        // eventB: only one data message (seq 11) before its boundary.
        assertThat(eventBBoundary.getLong(56, LE)).isEqualTo(11L); // firstSequenceIdOffset
        assertThat(eventBBoundary.getLong(64, LE)).isEqualTo(11L); // lastSequenceIdOffset

        // eventA: two data messages (seq 10, 12) before its boundary, despite eventB interleaving.
        assertThat(eventABoundary.getLong(56, LE)).isEqualTo(10L);
        assertThat(eventABoundary.getLong(64, LE)).isEqualTo(12L);
    }

    @Test
    void trackingEntryIsRemovedOnceTheBoundaryIsProcessed() {
        stamper = newStamper();
        UnsafeBuffer msg1 = message(DATA_TEMPLATE_ID, 5L);
        UnsafeBuffer boundary = message(BOUNDARY_TEMPLATE_ID, 5L);
        // A second, unrelated event reusing... a *different* eventId after the first completes.
        UnsafeBuffer laterBoundaryWithStaleEventId = message(BOUNDARY_TEMPLATE_ID, 5L);

        stamper.stamp(msg1, 1L);
        stamper.stamp(boundary, 2L);
        assertThat(boundary.getLong(56, LE)).isEqualTo(1L);
        assertThat(boundary.getLong(64, LE)).isEqualTo(1L);

        // eventId 5 was removed after its boundary; a stray later boundary with the same id
        // (should not normally happen, but must degrade safely) falls back to its own seq.
        stamper.stamp(laterBoundaryWithStaleEventId, 3L);
        assertThat(laterBoundaryWithStaleEventId.getLong(56, LE)).isEqualTo(3L);
        assertThat(laterBoundaryWithStaleEventId.getLong(64, LE)).isEqualTo(3L);
    }
}
