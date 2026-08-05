package gcm.md.sequencer.stamping;

import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * v3 compatibility profile (design §4, ported from phase-1 §3/§15): stamp offset 8 (sequenceId)
 * always; stamp offset 32 (sequenceTimestamp) only for templateIds in
 * {@code timestampTemplateIds} (v3 default: [1, 9]). No boundary enrichment — v3 never enables
 * it, and {@link StampingConfig#v3Profile} also disables source tracking, which predates both.
 */
class StampingEngineV3ProfileTest {

    private static final ByteOrder LE = ByteOrder.LITTLE_ENDIAN;

    private UnsafeBuffer v3Fixture(int templateId) {
        UnsafeBuffer buffer = new UnsafeBuffer(ByteBuffer.allocateDirect(64));
        buffer.putShort(2, (short) templateId, LE); // templateIdOffset
        buffer.putShort(4, (short) 100, LE);         // schemaIdOffset, matches default schemaId
        return buffer;
    }

    private StampingEngine v3Engine() {
        StampingConfig cfg = StampingConfig.v3Profile(Set.of(1, 9));
        return new StampingEngine(cfg, new EngineListener() {
        });
    }

    @Test
    void sequenceIdIsAlwaysStampedRegardlessOfTemplate() {
        StampingEngine engine = v3Engine();
        UnsafeBuffer buffer = v3Fixture(6); // not in the timestamp-template-ids list
        assertThat(engine.onMessage(buffer, 0, buffer.capacity(), 1L, 1L)).isEqualTo(Verdict.STAMPED);
        assertThat(buffer.getLong(8, LE)).isEqualTo(1L);
    }

    @Test
    void sequenceTimestampIsStampedOnlyForConfiguredTemplateIds() {
        StampingEngine engine = v3Engine();

        UnsafeBuffer included = v3Fixture(9);
        engine.onMessage(included, 0, included.capacity(), 555L, 1L);
        assertThat(included.getLong(32, LE)).isEqualTo(555L);

        UnsafeBuffer excluded = v3Fixture(6);
        engine.onMessage(excluded, 0, excluded.capacity(), 555L, 1L);
        assertThat(excluded.getLong(32, LE)).isZero();
    }
}
