package gcm.md.natsbridge.bridge;

import io.aeron.Aeron;
import io.aeron.archive.client.AeronArchive;
import org.agrona.CloseHelper;

import java.util.List;
import java.util.function.Function;

/**
 * Finds which cluster member currently has the live egress recording and returns a connected
 * {@link AeronArchive} to it. Archive control connections, unlike {@code AeronCluster} ingress
 * sessions, have no built-in leader-following — {@code libs/cluster-client}'s protocol contacts
 * every member and follows redirects automatically; Archive control is a plain point-to-point
 * session to whichever endpoint you give it. Since {@code SuppressionGate} only lets the current
 * leader publish (and therefore record) the egress stream, a single fixed archive connection
 * pointed at a bare round-robin headless-Service DNS name can land on a follower's *empty*
 * archive — {@code findLastMatchingRecording} would then wrongly report "nothing recorded yet"
 * even when the actual leader has a full recording, causing {@code BridgePipeline} to skip the
 * replay-catch-up path it should have taken. This was a real, previously-unfixed bug: confirmed
 * (docs/AERON-SEQUENCER-3-MEMBER-CLUSTER-STATUS.md) that {@code nats-bridge}'s
 * {@code archiveControlChannel} used exactly this bare-DNS-name pattern.
 *
 * <p>Tries each member's archive control channel in turn (design §5.2's per-member port scheme),
 * keeps the connection to whichever one actually has a matching recording, and closes the rest.
 * If none do (a genuinely fresh cluster with no egress recorded yet), keeps the first
 * successfully-connected candidate — {@code BridgePipeline} already handles "no recording found"
 * as its own valid live-only case, so any live connection works for that path. A candidate whose
 * member is currently unreachable (e.g. a follower pod between restarts) is skipped, not fatal.
 */
public final class LeaderArchiveConnector {

    private final Function<AeronArchive.Context, AeronArchive> connector;

    public LeaderArchiveConnector() {
        this(AeronArchive::connect);
    }

    /** Test-only constructor: injects the connect strategy so tests never need a live Archive. */
    LeaderArchiveConnector(Function<AeronArchive.Context, AeronArchive> connector) {
        this.connector = connector;
    }

    /**
     * @param candidates one {@code AeronArchive.Context} per cluster member, already configured
     *                   with that member's own control/response channels.
     * @return a connected {@code AeronArchive} to whichever candidate has the matching recording
     *         (or the first reachable candidate if none do) — caller owns the returned
     *         connection's lifecycle (the other candidates, if any were connected, are already
     *         closed before this method returns).
     * @throws IllegalStateException if no candidate's member is reachable at all.
     */
    public AeronArchive connectToRecordingOwner(List<AeronArchive.Context> candidates, String liveDestination,
                                                 int egressStreamId, int egressSessionId) {
        AeronArchive fallback = null;
        for (AeronArchive.Context candidateContext : candidates) {
            AeronArchive candidate;
            try {
                candidate = connector.apply(candidateContext);
            } catch (RuntimeException unreachable) {
                continue; // this member's archive isn't reachable right now — try the next one
            }
            long recordingId = candidate.findLastMatchingRecording(0L, liveDestination, egressStreamId, egressSessionId);
            if (recordingId != Aeron.NULL_VALUE) {
                CloseHelper.quietClose(fallback);
                return candidate;
            }
            if (fallback == null) {
                fallback = candidate;
            } else {
                CloseHelper.quietClose(candidate);
            }
        }
        if (fallback == null) {
            throw new IllegalStateException("No cluster member's archive control channel was reachable");
        }
        return fallback;
    }
}
