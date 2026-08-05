package gcm.md.sequencer.integration.tools;

import gcm.md.sequencer.stamping.StampingConfig;
import org.junit.jupiter.api.Test;

import java.nio.ByteOrder;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unlike the {@code *IT} classes in the sibling {@code integration} package, this exercises pure
 * join/diff logic with no NATS, no Aeron, no cluster — real and enabled, not {@code @Disabled},
 * since nothing here depends on the unvalidated multi-member path.
 */
class ParallelRunDiffHarnessTest {

    private static final StampingConfig CONFIG = StampingConfig.v4Defaults();

    @Test
    void identicalPayloadsOutsideMaskedFieldsAreReportedEqual() {
        ParallelRunDiffHarness harness = new ParallelRunDiffHarness(CONFIG);

        byte[] phase1Payload = payload(1L, /*sequenceId*/ 100L, /*sequenceTimestamp*/ 111L);
        byte[] phase2Payload = payload(1L, /*sequenceId*/ 7L, /*sequenceTimestamp*/ 222L);

        harness.onPhase1Message(new CapturedMessage(1L, phase1Payload, Instant.now()));
        harness.onPhase2Message(new CapturedMessage(1L, phase2Payload, Instant.now()));

        DiffReport report = harness.finish();
        assertThat(report.isClean()).isTrue();
        assertThat(report.toSummaryText()).contains("RESULT: CLEAN").contains("matched, byte-identical").contains("1");
    }

    @Test
    void aRealDifferenceOutsideTheMaskedFieldsIsCaught() {
        ParallelRunDiffHarness harness = new ParallelRunDiffHarness(CONFIG);

        byte[] phase1Payload = payload(1L, 100L, 111L);
        byte[] phase2Payload = payload(1L, 7L, 222L);
        phase2Payload[20] = (byte) (phase1Payload[20] + 1); // corrupt an unmasked byte

        harness.onPhase2Message(new CapturedMessage(1L, phase2Payload, Instant.now()));
        harness.onPhase1Message(new CapturedMessage(1L, phase1Payload, Instant.now()));

        DiffReport report = harness.finish();
        assertThat(report.isClean()).isFalse();
        assertThat(report.toSummaryText()).contains("RESULT: FIDELITY REGRESSION DETECTED").contains("byte mismatch at offset 20");
    }

    @Test
    void aMessageSeenOnOnlyOneSideIsReportedAsAGap() {
        ParallelRunDiffHarness harness = new ParallelRunDiffHarness(CONFIG);

        harness.onPhase1Message(new CapturedMessage(5L, payload(5L, 1L, 1L), Instant.now()));
        // phase-2 side never arrives — simulates a message the phase-2 pipeline dropped.

        DiffReport report = harness.finish();
        assertThat(report.isClean()).isFalse();
        assertThat(report.toSummaryText()).contains("phase-1-only (missing from phase-2 output): 1");
    }

    private static byte[] payload(long sourceSeqNum, long sequenceId, long sequenceTimestamp) {
        byte[] buf = new byte[128];
        org.agrona.concurrent.UnsafeBuffer buffer = new org.agrona.concurrent.UnsafeBuffer(buf);
        buffer.putShort(CONFIG.templateIdOffset(), (short) 9, ByteOrder.LITTLE_ENDIAN);
        buffer.putShort(CONFIG.schemaIdOffset(), (short) CONFIG.schemaId(), ByteOrder.LITTLE_ENDIAN);
        buffer.putLong(CONFIG.sequenceIdOffset(), sequenceId, ByteOrder.LITTLE_ENDIAN);
        buffer.putLong(CONFIG.sequenceTimestampOffset(), sequenceTimestamp, ByteOrder.LITTLE_ENDIAN);
        buffer.putInt(64, (int) sourceSeqNum, ByteOrder.LITTLE_ENDIAN);
        return buf;
    }
}
