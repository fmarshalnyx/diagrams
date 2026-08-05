package gcm.md.sequencer.health;

import gcm.md.sequencer.core.SequencerPipeline;
import io.nats.client.Connection;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthIndicator;

/**
 * Readiness per project spec §10 and §13: ingress connected, egress connected, leadership held,
 * ingress subscribed (i.e. the full startup ordering has completed), and the first message ever
 * received passed the schemaId sanity guard.
 */
public final class SequencerHealthIndicator implements HealthIndicator {

    private final SequencerPipeline pipeline;
    private final Connection ingressConnection;
    private final Connection egressConnection;

    /** Wires the health check against the pipeline and the two NATS connections it drives. */
    public SequencerHealthIndicator(SequencerPipeline pipeline, Connection ingressConnection,
                                     Connection egressConnection) {
        this.pipeline = pipeline;
        this.ingressConnection = ingressConnection;
        this.egressConnection = egressConnection;
    }

    @Override
    public Health health() {
        boolean ingressConnected = ingressConnection.getStatus() == Connection.Status.CONNECTED;
        boolean egressConnected = egressConnection.getStatus() == Connection.Status.CONNECTED;
        boolean leader = pipeline.isActive();
        boolean schemaValid = pipeline.isSchemaValid();

        Health.Builder builder = (ingressConnected && egressConnected && leader && schemaValid)
                ? Health.up()
                : Health.down();
        return builder
                .withDetail("ingressConnected", ingressConnected)
                .withDetail("egressConnected", egressConnected)
                .withDetail("leadershipHeld", leader)
                .withDetail("schemaValid", schemaValid)
                .build();
    }
}
