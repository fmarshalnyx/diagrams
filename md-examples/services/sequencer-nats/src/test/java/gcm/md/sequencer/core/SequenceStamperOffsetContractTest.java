package gcm.md.sequencer.core;

import com.usb.gcm.md.sbe.MarketDataDeltaDecoder;
import com.usb.gcm.md.sbe.MarketDataDeltaEncoder;
import com.usb.gcm.md.sbe.MatchEventBoundaryDecoder;
import com.usb.gcm.md.sbe.MatchEventBoundaryEncoder;
import com.usb.gcm.md.sbe.MessageHeaderDecoder;
import com.usb.gcm.md.sbe.MessageHeaderEncoder;
import gcm.md.sequencer.config.SequencerProperties;
import gcm.md.sequencer.metrics.SequencerMetrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.agrona.concurrent.OffsetEpochNanoClock;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Proves that the configured stamping offsets (project spec §3, §12) land in the exact fields
 * the generated v4 SBE codecs expect — this test must fail if either drifts (spec §15).
 * SBE codecs are used here only, per the project convention that the hot path never decodes SBE.
 */
class SequenceStamperOffsetContractTest {

    private SequenceStamper newStamper(SequencerProperties properties) {
        return new SequenceStamper(properties, new OffsetEpochNanoClock(), new SequencerMetrics(new SimpleMeterRegistry()));
    }

    @Test
    void stampsSequenceIdAndTimestampAtTheFieldsGeneratedCodecsExpect_marketDataDelta() {
        UnsafeBuffer buffer = new UnsafeBuffer(ByteBuffer.allocateDirect(512));
        MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
        MarketDataDeltaEncoder encoder = new MarketDataDeltaEncoder();
        encoder.wrapAndApplyHeader(buffer, 0, headerEncoder);
        encoder.header().sequenceId(0).sourceTimestamp(111).ingestTimestamp(222).sequenceTimestamp(0).eventId(0).reserved1(0);
        encoder.instrumentId(42).sourceSeqNum(7);

        SequenceStamper stamper = newStamper(new SequencerProperties());
        boolean ok = stamper.stamp(buffer, 999L);
        assertThat(ok).isTrue();

        MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();
        MarketDataDeltaDecoder decoder = new MarketDataDeltaDecoder();
        decoder.wrapAndApplyHeader(buffer, 0, headerDecoder);

        assertThat(decoder.header().sequenceId()).isEqualTo(999L);
        assertThat(decoder.header().sequenceTimestamp()).isGreaterThan(0L);
        // Fields the sequencer must never touch are untouched.
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

        SequencerProperties properties = new SequencerProperties();
        SequenceStamper stamper = newStamper(properties);

        // Prime the tracked event with two prior data messages sharing eventId=555, seq 100 and 101.
        UnsafeBuffer prior = new UnsafeBuffer(ByteBuffer.allocateDirect(512));
        MarketDataDeltaEncoder priorEncoder = new MarketDataDeltaEncoder();
        priorEncoder.wrapAndApplyHeader(prior, 0, new MessageHeaderEncoder());
        priorEncoder.header().sequenceId(0).sourceTimestamp(0).ingestTimestamp(0).sequenceTimestamp(0).eventId(555).reserved1(0);
        priorEncoder.instrumentId(1).sourceSeqNum(1);
        stamper.stamp(prior, 100L);
        stamper.stamp(prior, 101L);

        boolean ok = stamper.stamp(buffer, 102L);
        assertThat(ok).isTrue();

        MatchEventBoundaryDecoder decoder = new MatchEventBoundaryDecoder();
        decoder.wrapAndApplyHeader(buffer, 0, new MessageHeaderDecoder());
        assertThat(decoder.header().sequenceId()).isEqualTo(102L);
        assertThat(decoder.firstSequenceId()).isEqualTo(100L);
        assertThat(decoder.lastSequenceId()).isEqualTo(101L);
    }

    @Test
    void dropsAMessageWhoseSchemaIdDoesNotMatch() {
        UnsafeBuffer buffer = new UnsafeBuffer(ByteBuffer.allocateDirect(512));
        MarketDataDeltaEncoder encoder = new MarketDataDeltaEncoder();
        encoder.wrapAndApplyHeader(buffer, 0, new MessageHeaderEncoder());
        // Corrupt the schemaId field (abs offset 4) so it no longer matches configured 100.
        buffer.putShort(4, (short) 999, java.nio.ByteOrder.LITTLE_ENDIAN);

        SequenceStamper stamper = newStamper(new SequencerProperties());
        assertThat(stamper.stamp(buffer, 1L)).isFalse();
    }
}
