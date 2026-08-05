package gcm.md.sequencer.clusterclient;

/**
 * {@link ClusterIngressClient} retry/backoff tuning (design §7). Cluster connection settings
 * (ingress channel, member endpoints, credentials) are configured directly on the
 * {@code AeronCluster.Context} the caller supplies — this module doesn't re-invent config Aeron
 * already provides.
 *
 * @param maxBackpressureAttempts bounded retry count on {@code BACK_PRESSURED}/{@code ADMIN_ACTION}
 *                                before {@link ClusterIngressClient#offer} surfaces the
 *                                backpressure to the caller (design §7: "line handlers must be
 *                                able to spill to a local buffer or slow their feed handler" —
 *                                this client never blocks unboundedly, unlike the egress side).
 * @param backpressureIdleNanos   idle sleep between spin bursts while offer remains back-pressured.
 */
public record IngressClientConfig(int maxBackpressureAttempts, long backpressureIdleNanos) {

    public static IngressClientConfig defaults() {
        return new IngressClientConfig(1_000, 1_000_000L); // 1ms idle
    }
}
