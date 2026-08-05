package gcm.md.sequencer.integration.tools;

import gcm.md.sequencer.stamping.StampingConfig;

import java.util.Optional;

/**
 * Design §12.4: "diff everything except sequenceId/sequenceTimestamp" — the two fields every
 * sequencer, phase-1 or phase-2, is expected to stamp differently (different sequence spaces,
 * different clocks) even when fed byte-identical input. Every other byte of the two phases'
 * output for the same {@code sourceSeqNum} must match exactly, or the phase-2 migration has a
 * fidelity regression.
 */
final class MessageDiffer {

    private final int sequenceIdOffset;
    private final int sequenceTimestampOffset;

    MessageDiffer(StampingConfig stampingConfig) {
        this.sequenceIdOffset = stampingConfig.sequenceIdOffset();
        this.sequenceTimestampOffset = stampingConfig.sequenceTimestampOffset();
    }

    /** Empty if the two payloads are identical outside the masked sequenceId/sequenceTimestamp fields. */
    Optional<String> diff(byte[] phase1Payload, byte[] phase2Payload) {
        if (phase1Payload.length != phase2Payload.length) {
            return Optional.of("length mismatch: phase1=" + phase1Payload.length + " phase2=" + phase2Payload.length);
        }
        for (int i = 0; i < phase1Payload.length; i++) {
            if (isMasked(i)) {
                continue;
            }
            if (phase1Payload[i] != phase2Payload[i]) {
                return Optional.of("byte mismatch at offset " + i
                        + ": phase1=0x" + Integer.toHexString(phase1Payload[i] & 0xFF)
                        + " phase2=0x" + Integer.toHexString(phase2Payload[i] & 0xFF));
            }
        }
        return Optional.empty();
    }

    private boolean isMasked(int byteOffset) {
        return isWithinField(byteOffset, sequenceIdOffset) || isWithinField(byteOffset, sequenceTimestampOffset);
    }

    private static boolean isWithinField(int byteOffset, int fieldOffset) {
        return byteOffset >= fieldOffset && byteOffset < fieldOffset + Long.BYTES;
    }
}
