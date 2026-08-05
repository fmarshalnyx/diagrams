package gcm.md.mockupstreamsource.generate;

import java.util.Locale;

/**
 * Publish pacing shape (design: implementation-steps.md Milestone 5B). Gaps and duplicates are
 * controlled independently via {@code mock-upstream.gap-probability}/{@code duplicate-probability}
 * regardless of pattern — see {@link MessagePlanner}.
 */
public enum TrafficPattern {
    /** Constant inter-message interval derived from the configured rate. */
    STEADY,
    /** Alternating full-rate burst windows and quiet windows — simulates an intermittent feed. */
    BURSTY;

    public static TrafficPattern parse(String value) {
        return valueOf(value.trim().toUpperCase(Locale.ROOT));
    }
}
