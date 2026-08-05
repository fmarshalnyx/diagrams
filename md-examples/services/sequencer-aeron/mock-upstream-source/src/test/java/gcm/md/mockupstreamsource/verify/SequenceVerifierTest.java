package gcm.md.mockupstreamsource.verify;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class SequenceVerifierTest {

    @Test
    void noViolationsOnAContiguousNoDuplicateSequence() {
        SequenceVerifier verifier = new SequenceVerifier();
        for (long i = 1; i <= 100; i++) {
            verifier.record(i);
        }

        assertThat(verifier.duplicateCount()).isZero();
        assertThat(verifier.gapCount()).isZero();
        assertThat(verifier.hasViolations()).isFalse();
    }

    @Test
    void detectsAGap() {
        SequenceVerifier verifier = new SequenceVerifier();
        verifier.record(1);
        verifier.record(2);
        verifier.record(5); // 3, 4 missing

        assertThat(verifier.gapCount()).isEqualTo(2);
        assertThat(verifier.hasViolations()).isTrue();
    }

    @Test
    void detectsADuplicate() {
        SequenceVerifier verifier = new SequenceVerifier();
        verifier.record(1);
        verifier.record(2);
        verifier.record(2);

        assertThat(verifier.duplicateCount()).isEqualTo(1);
        assertThat(verifier.hasViolations()).isTrue();
    }

    @Test
    void combinesGapAndDuplicateViolations() {
        SequenceVerifier verifier = new SequenceVerifier();
        verifier.record(1);
        verifier.record(1);
        verifier.record(4);

        assertThat(verifier.duplicateCount()).isEqualTo(1);
        assertThat(verifier.gapCount()).isEqualTo(2); // 2, 3 missing between 1 and 4
        assertThat(verifier.hasViolations()).isTrue();
    }

    @Test
    void emptyVerifierHasNoViolations() {
        SequenceVerifier verifier = new SequenceVerifier();

        assertThat(verifier.gapCount()).isZero();
        assertThat(verifier.hasViolations()).isFalse();
    }
}
