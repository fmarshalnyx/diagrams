package gcm.md.sequencer.ingress;

import org.agrona.DirectBuffer;

/**
 * The one method a line handler needs to send a message into the sequencer, independent of which
 * concrete transport (Aeron cluster, NATS) is wired in at startup. Config selects the
 * implementation; the line handler's own code never branches on transport.
 *
 * <p>Implementations: {@code gcm.md.sequencer.clusterclient.ClusterIngressClient} (Aeron cluster,
 * {@code libs/cluster-client}) and {@code gcm.md.sequencer.egress.NatsIngressTransport} (NATS
 * JetStream, {@code libs/nats-egress}). Both are safe to swap purely by config because the
 * sequencer dedupes ingress on {@code sourceSeqNum} regardless of which transport delivered it
 * (design §4's per-source tracking) — see {@code libs/cluster-client}'s package-info for the
 * idempotency contract this relies on, which applies identically to the NATS path.
 */
public interface IngressTransport {

    /**
     * Offers one message. Must be called from a single thread.
     *
     * @return a non-negative value on success; a negative value means the offer was not
     *         accepted — the caller should back off and retry, or spill to a local buffer,
     *         rather than block indefinitely (a line handler's feed thread must stay free to
     *         slow its upstream feed). The exact negative values are implementation-specific
     *         (see the concrete transport's own Javadoc) — callers should only test {@code >= 0}
     *         vs {@code < 0}, never compare against a specific constant.
     */
    long offer(DirectBuffer buffer, int offset, int length);
}
