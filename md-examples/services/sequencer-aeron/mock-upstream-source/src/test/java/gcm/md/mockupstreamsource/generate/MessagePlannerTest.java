package gcm.md.mockupstreamsource.generate;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class MessagePlannerTest {

    @Test
    void reducesToPlainSequentialPublishWhenBothProbabilitiesAreZero() {
        MessagePlanner planner = new MessagePlanner(0.0, 0.0, 42L);

        for (long expected = 1; expected <= 100; expected++) {
            MessagePlanner.PlannedMessage planned = planner.next();
            assertThat(planned.decision()).isEqualTo(MessagePlanner.Decision.PUBLISH);
            assertThat(planned.sourceSeqNum()).isEqualTo(expected);
        }
    }

    @Test
    void aGivenSeedAlwaysProducesTheIdenticalDecisionSequence() {
        MessagePlanner first = new MessagePlanner(0.3, 0.2, 7L);
        MessagePlanner second = new MessagePlanner(0.3, 0.2, 7L);

        for (int i = 0; i < 200; i++) {
            assertThat(first.next()).isEqualTo(second.next());
        }
    }

    @Test
    void gapProbabilityOneAlwaysSkips() {
        MessagePlanner planner = new MessagePlanner(1.0, 0.0, 1L);

        for (int i = 0; i < 20; i++) {
            assertThat(planner.next().decision()).isEqualTo(MessagePlanner.Decision.SKIP);
        }
    }

    @Test
    void duplicateProbabilityOneAlwaysDuplicatesWhenGapProbabilityIsZero() {
        MessagePlanner planner = new MessagePlanner(0.0, 1.0, 1L);

        for (int i = 0; i < 20; i++) {
            assertThat(planner.next().decision()).isEqualTo(MessagePlanner.Decision.PUBLISH_THEN_DUPLICATE);
        }
    }

    @Test
    void sourceSeqNumAlwaysAdvancesByOnePerTickRegardlessOfDecision() {
        MessagePlanner planner = new MessagePlanner(0.5, 0.5, 99L);

        long previous = 0;
        for (int i = 0; i < 100; i++) {
            long current = planner.next().sourceSeqNum();
            assertThat(current).isEqualTo(previous + 1);
            previous = current;
        }
    }
}
