package gcm.md.sequencer.cluster;

/**
 * {@link AeronEgressPublisher} configuration (design §6.1, §6.2).
 *
 * @param egressChannel        the MDC dynamic control channel (e.g.
 *                              {@code aeron:udp?control=host:port|control-mode=dynamic}). N
 *                              subscribers (bridge, fast consumers, DR replicator) attach without
 *                              config changes; flipping to true multicast at colo changes only
 *                              this value (design §6.2).
 * @param egressStreamId        the stream id published on.
 * @param sequenceIdOffset      absolute byte offset of {@code sequenceId} — must match
 *                              {@link gcm.md.sequencer.stamping.StampingConfig#sequenceIdOffset()}.
 * @param backpressureMaxSpins  bounded spin count on {@code Publication.offer} before idling
 *                              (design §6.4: "bounded spin-then-idle").
 * @param backpressureIdleNanos idle sleep between spin bursts while offer remains back-pressured.
 * @param backpressureStallAlarmNanos duration past which an ongoing stall is treated as an alarm
 *                              condition rather than routine back-pressure.
 */
public record AeronEgressConfig(
        String egressChannel,
        int egressStreamId,
        int sequenceIdOffset,
        int backpressureMaxSpins,
        long backpressureIdleNanos,
        long backpressureStallAlarmNanos) {

    /** Local single-member defaults, matching {@link ClusterNodeConfig#localSingleMember()}. */
    public static AeronEgressConfig localDefaults() {
        return forHost("localhost");
    }

    /**
     * Defaults addressable at {@code host} (design §10: a pod's own headless-service DNS name in
     * Kubernetes) — see {@link ClusterNodeConfig#singleMember} for why this matters cross-pod.
     */
    public static AeronEgressConfig forHost(String host) {
        return new AeronEgressConfig(
                "aeron:udp?control=" + host + ":9070|control-mode=dynamic",
                1,
                8,
                1_000,
                1_000_000L, // 1ms
                500_000_000L); // 500ms, matching phase-1's egress.max-stall-ms default
    }
}
