package gcm.md.sequencer.cluster;

import io.aeron.cluster.service.Cluster;
import org.agrona.DirectBuffer;

/**
 * The cluster's sequenced output (design §6). {@link AeronEgressPublisher} is the real
 * implementation (Milestone 3); {@link NoOpEgressPublisher} is a stub used where no egress is
 * needed (tests, or a launcher profile that only needs to prove cluster/session/stamping wiring
 * — Milestone 2's acceptance scope).
 */
public interface EgressPublisher {

    /** Called once from {@code onStart}, with the live {@link Cluster} (e.g. for {@code cluster.aeron()}). */
    void onStart(Cluster cluster);

    /**
     * Publishes one stamped message carrying {@code sequenceId}. Called only from the clustered
     * service's single thread. Implementations gate this on leadership/replay state (design
     * §6.4) — a call here is not a guarantee the message is actually emitted.
     */
    void publish(DirectBuffer buffer, int offset, int length, long sequenceId);

    /** Notified on leadership change (design §6.4) so the real implementation can gate publishing. */
    void onRoleChange(Cluster.Role role);

    /** Called once from {@code onTerminate}; releases any held resources (publications, archive connections). */
    void onTerminate();
}
