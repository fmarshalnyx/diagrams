package gcm.md.mockupstreamsource.generate;

import java.util.Random;

/**
 * Decides, per tick, whether the next {@code sourceSeqNum} gets published normally, skipped
 * (a deliberate gap), or published then immediately re-published byte-identical (a deliberate
 * duplicate) — independent of {@link TrafficPattern}, which only controls publish pacing. Pure
 * and seeded, so a given {@code seed} always produces the identical published/skipped/duplicated
 * sequence — directly unit-testable without any NATS/IO dependency.
 *
 * <p>{@code gapProbability}/{@code duplicateProbability} default to {@code 0.0}, which reduces
 * this planner to plain {@code 1, 2, 3, ...} with every tick a normal {@link Decision#PUBLISH}.
 */
public final class MessagePlanner {

    public enum Decision {
        PUBLISH,
        SKIP,
        PUBLISH_THEN_DUPLICATE
    }

    /** @param sourceSeqNum the seqNum this tick consumed — SKIP still consumes one, creating a real gap. */
    public record PlannedMessage(Decision decision, long sourceSeqNum) {
    }

    private final double gapProbability;
    private final double duplicateProbability;
    private final Random random;
    private long counter;

    public MessagePlanner(double gapProbability, double duplicateProbability, long seed) {
        this.gapProbability = gapProbability;
        this.duplicateProbability = duplicateProbability;
        this.random = new Random(seed);
    }

    public PlannedMessage next() {
        long sourceSeqNum = ++counter;
        if (gapProbability > 0 && random.nextDouble() < gapProbability) {
            return new PlannedMessage(Decision.SKIP, sourceSeqNum);
        }
        if (duplicateProbability > 0 && random.nextDouble() < duplicateProbability) {
            return new PlannedMessage(Decision.PUBLISH_THEN_DUPLICATE, sourceSeqNum);
        }
        return new PlannedMessage(Decision.PUBLISH, sourceSeqNum);
    }
}
