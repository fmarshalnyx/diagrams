package gcm.md.mockupstreamsource.generate;

import com.usb.gcm.md.sbe.AssetClass;
import com.usb.gcm.md.sbe.MarketDataDeltaEncoder;
import com.usb.gcm.md.sbe.MessageHeaderEncoder;
import org.agrona.concurrent.UnsafeBuffer;

import java.time.Instant;

/**
 * Builds one canned, unsequenced {@code MarketDataDelta} SBE message (same shape
 * {@code sequencer-loadgen} used) via the generated codec — never hand-rolled offsets — so the
 * wire format stays correct even if the schema changes. {@code sequenceId} is left at {@code 0}:
 * the sequencer stamps it, this tool never does.
 */
public final class MessageEncoder {

    private static final int MESSAGE_LENGTH = 128;

    public byte[] encode(long sourceSeqNum) {
        byte[] data = new byte[MESSAGE_LENGTH];
        UnsafeBuffer buffer = new UnsafeBuffer(data);
        MarketDataDeltaEncoder encoder = new MarketDataDeltaEncoder();
        encoder.wrapAndApplyHeader(buffer, 0, new MessageHeaderEncoder());
        Instant now = Instant.now();
        long epochNanos = now.getEpochSecond() * 1_000_000_000L + now.getNano();
        encoder.header()
                .sequenceId(0)
                .sourceTimestamp(epochNanos)
                .ingestTimestamp(epochNanos)
                .sequenceTimestamp(0)
                .eventId(0)
                .reserved1(0);
        encoder.fieldPresence().clear();
        encoder.instrumentId(1);
        encoder.sourceSeqNum((int) sourceSeqNum);
        encoder.assetClass(AssetClass.ENERGY);
        encoder.changedFieldsCount(0);
        return data;
    }
}
