package gcm.md.linehandlertemplate.relay;

import org.agrona.MutableDirectBuffer;

import java.nio.ByteOrder;

/**
 * Writes {@code sourceSeqNum} into a message buffer at a configured offset — the one field a
 * line handler is responsible for, everything else in the payload is opaque to this template.
 * Pure and dependency-free so it's directly unit-testable without any Aeron/NATS/Spring context.
 */
public final class SourceSeqNumStamper {

    private static final ByteOrder LE = ByteOrder.LITTLE_ENDIAN;
    private static final long UINT32_MAX = 0xFFFFFFFFL;

    private final int offset;

    public SourceSeqNumStamper(int offset) {
        this.offset = offset;
    }

    /**
     * @throws IllegalArgumentException if {@code sourceSeqNum} exceeds what the wire field (a
     *         uint32, matching {@code StampingConfig}'s v4 schema) can represent — silently
     *         wrapping around would be a much worse failure mode than failing loudly here.
     */
    public void stamp(MutableDirectBuffer buffer, long sourceSeqNum) {
        if (sourceSeqNum < 0 || sourceSeqNum > UINT32_MAX) {
            throw new IllegalArgumentException(
                    "sourceSeqNum " + sourceSeqNum + " does not fit the uint32 wire field at offset " + offset);
        }
        buffer.putInt(offset, (int) sourceSeqNum, LE);
    }
}
