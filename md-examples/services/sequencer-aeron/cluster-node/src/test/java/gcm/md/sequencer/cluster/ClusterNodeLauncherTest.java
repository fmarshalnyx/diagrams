package gcm.md.sequencer.cluster;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@link ClusterNodeLauncher#parsePodOrdinal} is pure string logic — the only piece of the
 * Kubernetes N-member branch cheap to unit test without a live StatefulSet.
 */
class ClusterNodeLauncherTest {

    @Test
    void parsesTheOrdinalSuffixFromAStatefulSetPodName() {
        assertThat(ClusterNodeLauncher.parsePodOrdinal("gcm-md-seq-cluster-node-0")).isZero();
        assertThat(ClusterNodeLauncher.parsePodOrdinal("gcm-md-seq-cluster-node-1")).isEqualTo(1);
        assertThat(ClusterNodeLauncher.parsePodOrdinal("gcm-md-seq-cluster-node-12")).isEqualTo(12);
    }

    @Test
    void rejectsANullPodName() {
        assertThatThrownBy(() -> ClusterNodeLauncher.parsePodOrdinal(null))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("POD_NAME is missing");
    }

    @Test
    void rejectsABlankPodName() {
        assertThatThrownBy(() -> ClusterNodeLauncher.parsePodOrdinal("  "))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("POD_NAME is missing");
    }

    @Test
    void rejectsAPodNameWithNoDash() {
        assertThatThrownBy(() -> ClusterNodeLauncher.parsePodOrdinal("clusternode"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("does not match the expected");
    }

    @Test
    void rejectsAPodNameWithATrailingDashAndNothingAfter() {
        assertThatThrownBy(() -> ClusterNodeLauncher.parsePodOrdinal("gcm-md-seq-cluster-node-"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("does not match the expected");
    }

    @Test
    void rejectsAPodNameWithANonNumericSuffix() {
        assertThatThrownBy(() -> ClusterNodeLauncher.parsePodOrdinal("gcm-md-seq-cluster-node-abc"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("does not end in a numeric");
    }
}
