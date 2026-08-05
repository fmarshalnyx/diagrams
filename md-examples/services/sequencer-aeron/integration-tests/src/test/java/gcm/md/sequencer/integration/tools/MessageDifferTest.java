package gcm.md.sequencer.integration.tools;

import gcm.md.sequencer.stamping.StampingConfig;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Direct coverage of {@link MessageDiffer}'s masking boundaries — {@link ParallelRunDiffHarnessTest}
 * already exercises the happy/unhappy paths end-to-end through the harness, but the exact byte
 * boundaries of the masked {@code sequenceId}/{@code sequenceTimestamp} fields are the one thing a
 * §12.4 diff run absolutely cannot get wrong (mask one byte too few and every real run false-alarms
 * on every message; one byte too many and a genuine regression goes unreported).
 */
class MessageDifferTest {

    private static final StampingConfig CONFIG = StampingConfig.v4Defaults(); // sequenceIdOffset=8, sequenceTimestampOffset=32
    private final MessageDiffer differ = new MessageDiffer(CONFIG);

    @Test
    void payloadsDifferingOnlyInsideTheSequenceIdFieldAreEqual() {
        byte[] a = new byte[64];
        byte[] b = new byte[64];
        for (int i = CONFIG.sequenceIdOffset(); i < CONFIG.sequenceIdOffset() + Long.BYTES; i++) {
            b[i] = (byte) 0xFF;
        }

        assertThat(differ.diff(a, b)).isEmpty();
    }

    @Test
    void payloadsDifferingOnlyInsideTheSequenceTimestampFieldAreEqual() {
        byte[] a = new byte[64];
        byte[] b = new byte[64];
        for (int i = CONFIG.sequenceTimestampOffset(); i < CONFIG.sequenceTimestampOffset() + Long.BYTES; i++) {
            b[i] = (byte) 0xFF;
        }

        assertThat(differ.diff(a, b)).isEmpty();
    }

    @Test
    void aDifferenceOneByteBeforeTheSequenceIdFieldIsCaught() {
        byte[] a = new byte[64];
        byte[] b = new byte[64];
        b[CONFIG.sequenceIdOffset() - 1] = (byte) 0x01;

        Optional<String> diff = differ.diff(a, b);
        assertThat(diff).isPresent();
        assertThat(diff.get()).contains("offset " + (CONFIG.sequenceIdOffset() - 1));
    }

    @Test
    void aDifferenceOneByteAfterTheSequenceIdFieldIsCaught() {
        byte[] a = new byte[64];
        byte[] b = new byte[64];
        int firstByteAfterField = CONFIG.sequenceIdOffset() + Long.BYTES;
        b[firstByteAfterField] = (byte) 0x01;

        Optional<String> diff = differ.diff(a, b);
        assertThat(diff).isPresent();
        assertThat(diff.get()).contains("offset " + firstByteAfterField);
    }

    @Test
    void differingLengthsAreReportedWithoutIndexingOutOfBounds() {
        byte[] a = new byte[64];
        byte[] b = new byte[32];

        Optional<String> diff = differ.diff(a, b);
        assertThat(diff).isPresent();
        assertThat(diff.get()).contains("length mismatch").contains("64").contains("32");
    }

    @Test
    void byteIdenticalPayloadsAreEqual() {
        byte[] a = {1, 2, 3, 4, 5};
        byte[] b = {1, 2, 3, 4, 5};

        assertThat(differ.diff(a, b)).isEmpty();
    }
}
