package gcm.md.mockupstreamsource.generate;

import com.usb.gcm.md.sbe.AssetClass;
import com.usb.gcm.md.sbe.MarketDataDeltaDecoder;
import com.usb.gcm.md.sbe.MessageHeaderDecoder;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

import java.nio.ByteOrder;

import static org.assertj.core.api.Assertions.assertThat;

class MessageEncoderTest {

    private static final ByteOrder LE = ByteOrder.LITTLE_ENDIAN;
    /** StampingConfig.SourceTrackingConfig.v4Defaults(): sourceSeqNum at abs offset 64 for templateId 9. */
    private static final int SOURCE_SEQ_NUM_OFFSET = 64;

    @Test
    void sourceSeqNumRoundTripsThroughTheGeneratedCodec() {
        byte[] data = new MessageEncoder().encode(12345L);

        UnsafeBuffer buffer = new UnsafeBuffer(data);
        MarketDataDeltaDecoder decoder = new MarketDataDeltaDecoder();
        decoder.wrapAndApplyHeader(buffer, 0, new MessageHeaderDecoder());

        assertThat(decoder.sourceSeqNum()).isEqualTo(12345L);
        assertThat(decoder.instrumentId()).isEqualTo(1L);
        assertThat(decoder.assetClass()).isEqualTo(AssetClass.ENERGY);
    }

    @Test
    void sourceSeqNumAlsoLandsAtTheFixedHotPathOffset() {
        byte[] data = new MessageEncoder().encode(99L);

        UnsafeBuffer buffer = new UnsafeBuffer(data);
        assertThat(buffer.getInt(SOURCE_SEQ_NUM_OFFSET, LE) & 0xFFFFFFFFL).isEqualTo(99L);
    }

    @Test
    void sequenceIdIsLeftAtZeroForTheSequencerToStamp() {
        byte[] data = new MessageEncoder().encode(1L);

        UnsafeBuffer buffer = new UnsafeBuffer(data);
        MarketDataDeltaDecoder decoder = new MarketDataDeltaDecoder();
        decoder.wrapAndApplyHeader(buffer, 0, new MessageHeaderDecoder());

        assertThat(decoder.header().sequenceId()).isZero();
    }
}
