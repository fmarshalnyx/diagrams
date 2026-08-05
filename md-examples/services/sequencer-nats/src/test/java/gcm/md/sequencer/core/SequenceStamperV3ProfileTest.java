package gcm.md.sequencer.core;

import gcm.md.sequencer.config.SequencerProperties;
import gcm.md.sequencer.metrics.SequencerMetrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.agrona.concurrent.OffsetEpochNanoClock;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * v3 compatibility mode (project spec §3, §15): stamp offset 8 (sequenceId) always; stamp
 * offset 32 (sequenceTimestamp) only for templateIds in {@code stamping.timestamp-template-ids}
 * (v3 default: [1, 9]). No boundary enrichment fixtures needed — v3 never enables it.
 */
class SequenceStamperV3ProfileTest {

    private static final ByteOrder LE = ByteOrder.LITTLE_ENDIAN;

    private UnsafeBuffer v3Fixture(int templateId) {
        UnsafeBuffer buffer = new UnsafeBuffer(ByteBuffer.allocateDirect(64));
        buffer.putShort(2, (short) templateId, LE); // templateIdOffset
        buffer.putShort(4, (short) 100, LE);         // schemaIdOffset, matches default schemaId
        return buffer;
    }

    private SequenceStamper v3Stamper() {
        SequencerProperties properties = new SequencerProperties();
        properties.getStamping().setProfile("v3");
        properties.getStamping().setTimestampTemplateIds(List.of(1, 9));
        properties.getStamping().getEventEnrichment().setEnabled(false);
        return new SequenceStamper(properties, new OffsetEpochNanoClock(), new SequencerMetrics(new SimpleMeterRegistry()));
    }

    @Test
    void sequenceIdIsAlwaysStampedRegardlessOfTemplate() {
        SequenceStamper stamper = v3Stamper();
        UnsafeBuffer buffer = v3Fixture(6); // not in the timestamp-template-ids list
        assertThat(stamper.stamp(buffer, 42L)).isTrue();
        assertThat(buffer.getLong(8, LE)).isEqualTo(42L);
    }

    @Test
    void sequenceTimestampIsStampedOnlyForConfiguredTemplateIds() {
        SequenceStamper stamper = v3Stamper();

        UnsafeBuffer included = v3Fixture(9);
        stamper.stamp(included, 1L);
        assertThat(included.getLong(32, LE)).isNotZero();

        UnsafeBuffer excluded = v3Fixture(6);
        stamper.stamp(excluded, 2L);
        assertThat(excluded.getLong(32, LE)).isZero();
    }
}
