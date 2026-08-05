package gcm.md.sequencer.integration.tools;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Direct coverage of {@link DiffReport}'s accumulation and the CI-gate-relevant {@link DiffReport#isClean()}. */
class DiffReportTest {

    @Test
    void freshReportIsClean() {
        DiffReport report = new DiffReport();

        assertThat(report.isClean()).isTrue();
        assertThat(report.toSummaryText()).contains("RESULT: CLEAN");
    }

    @Test
    void aRecordedDifferenceMakesTheReportUnclean() {
        DiffReport report = new DiffReport();
        report.recordEqual(1L);
        report.recordDifferent(2L, "byte mismatch at offset 20");

        assertThat(report.isClean()).isFalse();
        assertThat(report.toSummaryText())
                .contains("RESULT: FIDELITY REGRESSION DETECTED")
                .contains("matched, byte-identical outside sequenceId/sequenceTimestamp: 1")
                .contains("matched, but differed elsewhere: 1")
                .contains("sourceSeqNum=2")
                .contains("byte mismatch at offset 20");
    }

    @Test
    void aPhase1OnlyEntryMakesTheReportUncleanAndListsTheKey() {
        DiffReport report = new DiffReport();
        report.recordPhase1Only(42L);

        assertThat(report.isClean()).isFalse();
        assertThat(report.toSummaryText())
                .contains("phase-1-only (missing from phase-2 output): 1")
                .contains("[42]");
    }

    @Test
    void aPhase2OnlyEntryMakesTheReportUncleanAndListsTheKey() {
        DiffReport report = new DiffReport();
        report.recordPhase2Only(7L);

        assertThat(report.isClean()).isFalse();
        assertThat(report.toSummaryText())
                .contains("phase-2-only (missing from phase-1 output): 1")
                .contains("[7]");
    }

    @Test
    void onlyKeysAreReportedInSortedOrder() {
        DiffReport report = new DiffReport();
        report.recordPhase1Only(30L);
        report.recordPhase1Only(10L);
        report.recordPhase1Only(20L);

        assertThat(report.toSummaryText()).contains("[10, 20, 30]");
    }
}
