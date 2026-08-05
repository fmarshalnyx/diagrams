package gcm.md.sequencer.egress;

/**
 * Transport tuning for {@link CoreNatsDestination} / {@link JetStreamDestination}, extracted
 * from the host's {@code sequencer.egress} config tree so this module carries no Spring
 * {@code @ConfigurationProperties} dependency.
 */
public record EgressConfig(String natsSubject, boolean blockOnBackpressure, long maxStallMs,
                            int jetStreamMaxInFlight) {
}
