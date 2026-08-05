package gcm.md.sequencer.egress;

/**
 * {@link NatsIngressTransport} tuning, extracted from the host's config tree so this module
 * carries no Spring {@code @ConfigurationProperties} dependency (mirrors {@link EgressConfig}).
 *
 * @param natsSubject the subject to publish to (phase-1's {@code MD_RAW}, or equivalent).
 * @param maxInFlight bounded async publish-ack window; a full window is surfaced to the caller
 *                    as backpressure (see {@link NatsIngressTransport#offer}) rather than
 *                    blocked or dropped, matching {@code ClusterIngressClient}'s ingress
 *                    contract so a line handler's config can select either transport
 *                    interchangeably.
 */
public record NatsIngressConfig(String natsSubject, int maxInFlight) {

    public static NatsIngressConfig defaults(String natsSubject) {
        return new NatsIngressConfig(natsSubject, 10_000);
    }
}
