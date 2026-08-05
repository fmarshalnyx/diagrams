package gcm.md.sequencer.clusterclient;

import gcm.md.sequencer.ingress.IngressTransport;
import io.aeron.Publication;
import io.aeron.cluster.client.AeronCluster;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;

import java.util.concurrent.locks.LockSupport;
import java.util.function.Function;

/**
 * Thin wrapper over {@link AeronCluster} — one of the two {@link IngressTransport} line handlers
 * can be configured with, alongside {@code gcm.md.sequencer.egress.NatsIngressTransport}
 * (design §7; see {@code libs/ingress-transport}'s package-info for why the transport is a
 * config choice rather than a staged migration through a shim service).
 *
 * <p><b>Idempotent by construction:</b> the clustered service dedupes ingress on
 * {@code sourceSeqNum} (design §4's per-source tracking). The crash-recovery contract for any
 * caller of this class is simply "republish your tail since last known-processed" — a
 * duplicate republish is a safe no-op on the server side, never a new sequenceId under a
 * different number. This supersedes phase-1's "redelivery gets a new sequenceId" caveat, which
 * becomes stale (and should be struck from downstream docs) once line handlers switch their
 * config to this transport.
 *
 * <p><b>Backpressure is surfaced, never absorbed by blocking.</b> Unlike
 * {@code AeronEgressPublisher}'s unbounded block-until-room retry, {@link #offer} retries only a
 * bounded number of times before returning the backpressure result to the caller — a line
 * handler's feed-handling thread must be free to spill to a local buffer or slow its upstream
 * feed instead of stalling indefinitely (design §7).
 */
public final class ClusterIngressClient implements IngressTransport, AutoCloseable {

    private final IngressClientConfig config;
    private final Function<AeronCluster.Context, AeronCluster> connector;
    private final AeronCluster.Context contextTemplate;
    private AeronCluster cluster;

    public ClusterIngressClient(AeronCluster.Context contextTemplate, IngressClientConfig config) {
        this(contextTemplate, config, AeronCluster::connect);
    }

    /** Test-only constructor: injects the connect strategy so tests never need a live cluster. */
    ClusterIngressClient(AeronCluster.Context contextTemplate, IngressClientConfig config,
                          Function<AeronCluster.Context, AeronCluster> connector) {
        this.contextTemplate = contextTemplate;
        this.config = config;
        this.connector = connector;
        this.cluster = connector.apply(contextTemplate.clone());
    }

    /**
     * Offers one message. Must be called from a single thread (matching {@link AeronCluster}'s
     * own single-threaded-client contract).
     *
     * @return a non-negative value on success; otherwise one of {@link Publication}'s negative
     *         result constants (e.g. {@code BACK_PRESSURED}) once bounded retry is exhausted, or
     *         {@code NOT_CONNECTED} immediately after a reconnect attempt — see class Javadoc for
     *         why this never blocks unboundedly.
     */
    @Override
    public long offer(DirectBuffer buffer, int offset, int length) {
        cluster.pollEgress();

        int attempts = 0;
        while (true) {
            long result = cluster.offer(buffer, offset, length);
            if (result >= 0L) {
                return result;
            }
            if (result == Publication.NOT_CONNECTED || result == Publication.CLOSED) {
                reconnect();
                return result;
            }
            if (result == Publication.MAX_POSITION_EXCEEDED) {
                return result;
            }
            // BACK_PRESSURED or ADMIN_ACTION: bounded retry only, then surface it.
            attempts++;
            if (attempts >= config.maxBackpressureAttempts()) {
                return result;
            }
            LockSupport.parkNanos(config.backpressureIdleNanos());
        }
    }

    /** Must be polled regularly even between offers, to process session/leadership control events. */
    public void pollEgress() {
        cluster.pollEgress();
    }

    /** Attempts a fresh connection, replacing the current (dropped) session. Safe to call repeatedly. */
    private void reconnect() {
        CloseHelper.quietClose(cluster);
        cluster = connector.apply(contextTemplate.clone());
    }

    @Override
    public void close() {
        CloseHelper.quietClose(cluster);
    }
}
