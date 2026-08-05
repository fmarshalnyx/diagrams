package gcm.md.sequencer.stamping;

import com.usb.gcm.md.sbe.MarketDataDeltaDecoder;
import com.usb.gcm.md.sbe.MarketDataDeltaEncoder;
import com.usb.gcm.md.sbe.MatchEventBoundaryDecoder;
import com.usb.gcm.md.sbe.MatchEventBoundaryEncoder;
import com.usb.gcm.md.sbe.MessageHeaderDecoder;
import com.usb.gcm.md.sbe.MessageHeaderEncoder;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Proves that the configured stamping offsets (design §4, ported from phase-1 §3/§12) land in
 * the exact fields the generated v4 SBE codecs expect — this test must fail if either drifts.
 * SBE codecs are used here only, per the module convention that the engine itself never decodes
 * SBE. Ported from phase-1's {@code SequenceStamperOffsetContractTest}, now against
 * {@link StampingEngine} directly with no Spring/metrics infrastructure.
 */
class StampingEngineOffsetContractTest {

    private static final long SOURCE_ID = 1L;

    private StampingEngine newEngine() {
        return new StampingEngine(StampingConfig.v4Defaults(), new EngineListener() {
        });
    }

    @Test
    void stampsSequenceIdAndTimestampAtTheFieldsGeneratedCodecsExpect_marketDataDelta() {
        UnsafeBuffer buffer = new UnsafeBuffer(ByteBuffer.allocateDirect(512));
        MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
        MarketDataDeltaEncoder encoder = new MarketDataDeltaEncoder();
        encoder.wrapAndApplyHeader(buffer, 0, headerEncoder);
        encoder.header().sequenceId(0).sourceTimestamp(111).ingestTimestamp(222).sequenceTimestamp(0).eventId(0).reserved1(0);
        encoder.instrumentId(42).sourceSeqNum(1);

        StampingEngine engine = newEngine();
        Verdict verdict = engine.onMessage(buffer, 0, buffer.capacity(), 999_000L, SOURCE_ID);
        assertThat(verdict).isEqualTo(Verdict.STAMPED);

        MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();
        MarketDataDeltaDecoder decoder = new MarketDataDeltaDecoder();
        decoder.wrapAndApplyHeader(buffer, 0, headerDecoder);

        assertThat(decoder.header().sequenceId()).isEqualTo(1L);
        assertThat(decoder.header().sequenceTimestamp()).isEqualTo(999_000L);
        // Fields the engine must never touch are untouched.
        assertThat(decoder.header().sourceTimestamp()).isEqualTo(111L);
        assertThat(decoder.header().ingestTimestamp()).isEqualTo(222L);
        assertThat(decoder.instrumentId()).isEqualTo(42L);
    }

    @Test
    void stampsMatchEventBoundaryFirstAndLastSequenceIdAtTheFieldsGeneratedCodecsExpect() {
        UnsafeBuffer buffer = new UnsafeBuffer(ByteBuffer.allocateDirect(512));
        MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
        MatchEventBoundaryEncoder encoder = new MatchEventBoundaryEncoder();
        encoder.wrapAndApplyHeader(buffer, 0, headerEncoder);
        encoder.header().sequenceId(0).sourceTimestamp(0).ingestTimestamp(0).sequenceTimestamp(0).eventId(555).reserved1(0);
        encoder.firstSequenceId(0).lastSequenceId(0).messageCount(3).eventFlags((short) 0).source("CME");

        StampingEngine engine = newEngine();

        // Prime the tracked event with two prior data messages sharing eventId=555.
        UnsafeBuffer prior = new UnsafeBuffer(ByteBuffer.allocateDirect(512));
        MarketDataDeltaEncoder priorEncoder = new MarketDataDeltaEncoder();
        priorEncoder.wrapAndApplyHeader(prior, 0, new MessageHeaderEncoder());
        priorEncoder.header().sequenceId(0).sourceTimestamp(0).ingestTimestamp(0).sequenceTimestamp(0).eventId(555).reserved1(0);
        priorEncoder.instrumentId(1).sourceSeqNum(1);
        engine.onMessage(prior, 0, prior.capacity(), 0L, SOURCE_ID); // seq 1

        UnsafeBuffer prior2 = new UnsafeBuffer(ByteBuffer.allocateDirect(512));
        MarketDataDeltaEncoder priorEncoder2 = new MarketDataDeltaEncoder();
        priorEncoder2.wrapAndApplyHeader(prior2, 0, new MessageHeaderEncoder());
        priorEncoder2.header().sequenceId(0).sourceTimestamp(0).ingestTimestamp(0).sequenceTimestamp(0).eventId(555).reserved1(0);
        priorEncoder2.instrumentId(1).sourceSeqNum(2);
        engine.onMessage(prior2, 0, prior2.capacity(), 0L, SOURCE_ID); // seq 2

        Verdict verdict = engine.onMessage(buffer, 0, buffer.capacity(), 0L, SOURCE_ID); // seq 3
        assertThat(verdict).isEqualTo(Verdict.STAMPED);

        MatchEventBoundaryDecoder decoder = new MatchEventBoundaryDecoder();
        decoder.wrapAndApplyHeader(buffer, 0, new MessageHeaderDecoder());
        assertThat(decoder.header().sequenceId()).isEqualTo(3L);
        assertThat(decoder.firstSequenceId()).isEqualTo(1L);
        assertThat(decoder.lastSequenceId()).isEqualTo(2L);
    }

    @Test
    void rejectsAMessageWhoseSchemaIdDoesNotMatch() {
        UnsafeBuffer buffer = new UnsafeBuffer(ByteBuffer.allocateDirect(512));
        MarketDataDeltaEncoder encoder = new MarketDataDeltaEncoder();
        encoder.wrapAndApplyHeader(buffer, 0, new MessageHeaderEncoder());
        // Corrupt the schemaId field (abs offset 4) so it no longer matches configured 100.
        buffer.putShort(4, (short) 999, java.nio.ByteOrder.LITTLE_ENDIAN);

        StampingEngine engine = newEngine();
        Verdict verdict = engine.onMessage(buffer, 0, buffer.capacity(), 1L, SOURCE_ID);
        assertThat(verdict).isEqualTo(Verdict.REJECTED_SCHEMA);
        assertThat(engine.currentSequenceId()).isZero();
    }
}
