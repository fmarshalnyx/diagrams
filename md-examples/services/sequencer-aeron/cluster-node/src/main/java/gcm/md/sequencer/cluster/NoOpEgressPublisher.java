package gcm.md.sequencer.cluster;

import io.aeron.cluster.service.Cluster;
import org.agrona.DirectBuffer;

/**
 * Placeholder {@link EgressPublisher} for contexts that don't need real egress (design §5
 * Milestone-2 acceptance: "egress publisher can be a no-op stub at this point"; also useful in
 * tests). {@link AeronEgressPublisher} is the real Milestone-3 implementation.
 */
public final class NoOpEgressPublisher implements EgressPublisher {

    @Override
    public void onStart(Cluster cluster) {
        // Intentionally no-op.
    }

    @Override
    public void publish(DirectBuffer buffer, int offset, int length, long sequenceId) {
        // Intentionally no-op.
    }

    @Override
    public void onRoleChange(Cluster.Role role) {
        // Intentionally no-op.
    }

    @Override
    public void onTerminate() {
        // Intentionally no-op.
    }
}
