package gcm.md.sequencer.cluster;

import io.aeron.cluster.service.Cluster;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The replay/failover suppression gate (design §6.4) — "no sequenced message is ever published
 * twice" is the phase-2 acceptance centerpiece, so this pure decision logic is tested
 * exhaustively and independently of any Aeron/Archive plumbing.
 */
class SuppressionGateTest {

    @Test
    void suppressesEverythingByDefaultBeforeAnyRoleIsKnown() {
        SuppressionGate gate = new SuppressionGate();
        assertThat(gate.shouldSuppress(1L)).isTrue();
    }

    @Test
    void followersSuppressEveryMessageRegardlessOfSequenceId() {
        SuppressionGate gate = new SuppressionGate();
        gate.onRoleChange(Cluster.Role.FOLLOWER, SuppressionGate.NO_SUPPRESSION);
        assertThat(gate.shouldSuppress(1L)).isTrue();
        assertThat(gate.shouldSuppress(1_000_000L)).isTrue();
    }

    @Test
    void candidatesSuppressEveryMessage() {
        SuppressionGate gate = new SuppressionGate();
        gate.onRoleChange(Cluster.Role.CANDIDATE, SuppressionGate.NO_SUPPRESSION);
        assertThat(gate.shouldSuppress(1L)).isTrue();
    }

    @Test
    void leaderWithNoPriorRecordingPublishesEverything() {
        SuppressionGate gate = new SuppressionGate();
        gate.onRoleChange(Cluster.Role.LEADER, SuppressionGate.NO_SUPPRESSION);
        assertThat(gate.shouldSuppress(1L)).isFalse();
        assertThat(gate.shouldSuppress(2L)).isFalse();
    }

    @Test
    void leaderCatchingUpSuppressesUpToAndIncludingTheLastPublishedSequenceId() {
        SuppressionGate gate = new SuppressionGate();
        gate.onRoleChange(Cluster.Role.LEADER, 95L); // a prior leader had published through seq 95

        assertThat(gate.shouldSuppress(90L)).isTrue();
        assertThat(gate.shouldSuppress(95L)).isTrue(); // boundary: already published, must not repeat
        assertThat(gate.shouldSuppress(96L)).isFalse(); // first message never actually published
    }

    @Test
    void resumesPublishingPermanentlyOncePastTheSuppressionFloorWithNoExplicitResumeStep() {
        SuppressionGate gate = new SuppressionGate();
        gate.onRoleChange(Cluster.Role.LEADER, 95L);

        assertThat(gate.shouldSuppress(96L)).isFalse();
        assertThat(gate.shouldSuppress(97L)).isFalse();
        assertThat(gate.shouldSuppress(200L)).isFalse();
    }

    @Test
    void losingLeadershipResetsSuppressionStateForTheNextTerm() {
        SuppressionGate gate = new SuppressionGate();
        gate.onRoleChange(Cluster.Role.LEADER, SuppressionGate.NO_SUPPRESSION);
        assertThat(gate.shouldSuppress(1L)).isFalse();

        gate.onRoleChange(Cluster.Role.FOLLOWER, SuppressionGate.NO_SUPPRESSION);
        assertThat(gate.shouldSuppress(2L)).isTrue();

        // Reassuming leadership must re-derive suppressUpTo fresh, not carry over stale state.
        gate.onRoleChange(Cluster.Role.LEADER, 10L);
        assertThat(gate.shouldSuppress(5L)).isTrue();
        assertThat(gate.shouldSuppress(11L)).isFalse();
    }

    @Test
    void exposesTheCurrentSuppressionFloorForDiagnostics() {
        SuppressionGate gate = new SuppressionGate();
        gate.onRoleChange(Cluster.Role.LEADER, 42L);
        assertThat(gate.suppressUpTo()).isEqualTo(42L);
    }
}
