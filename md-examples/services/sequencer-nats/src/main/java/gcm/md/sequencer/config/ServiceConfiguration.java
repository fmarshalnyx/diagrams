package gcm.md.sequencer.config;

import gcm.md.sequencer.core.LeaderElection;
import gcm.md.sequencer.core.SequenceAllocator;
import gcm.md.sequencer.core.SequenceStamper;
import gcm.md.sequencer.core.SequencerPipeline;
import gcm.md.sequencer.egress.BatchingConfig;
import gcm.md.sequencer.egress.BatchingDestination;
import gcm.md.sequencer.egress.CoreNatsDestination;
import gcm.md.sequencer.egress.DestinationChannel;
import gcm.md.sequencer.egress.EgressConfig;
import gcm.md.sequencer.egress.JetStreamDestination;
import gcm.md.sequencer.health.SequencerHealthIndicator;
import gcm.md.sequencer.heartbeat.HeartbeatEmitter;
import gcm.md.sequencer.ingress.IngressChannel;
import gcm.md.sequencer.ingress.NatsSequencerIngress;
import gcm.md.sequencer.metrics.SequencerMetrics;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import io.micrometer.core.instrument.MeterRegistry;
import io.nats.client.Connection;
import io.nats.client.KeyValueManagement;
import io.nats.client.Nats;
import org.agrona.concurrent.OffsetEpochNanoClock;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;

/**
 * Sole source of bean definitions for the {@code md-sequencer} service, per project convention:
 * no {@code @Autowired}, no stereotype-annotated business classes — every collaborator is wired
 * explicitly here.
 */
@Configuration
@EnableConfigurationProperties(SequencerProperties.class)
public class ServiceConfiguration {

    /** Calibrated epoch-nanos clock used for every {@code sequenceTimestamp} and heartbeat stamp. */
    @Bean
    public OffsetEpochNanoClock offsetEpochNanoClock() {
        return new OffsetEpochNanoClock();
    }

    /** All sequencer counters, gauges, and the off-thread latency histogram. */
    @Bean
    public SequencerMetrics sequencerMetrics(MeterRegistry meterRegistry) {
        return new SequencerMetrics(meterRegistry);
    }

    /** Dedicated NATS connection for ingress (spec §5; may point at the same cluster as egress). */
    @Bean(destroyMethod = "close")
    public Connection ingressConnection(SequencerProperties properties) throws IOException, InterruptedException {
        return Nats.connect(properties.getIngress().getNats().getUrl());
    }

    /** Dedicated NATS connection for egress and the allocator's KV bucket (spec §6, §7). */
    @Bean(destroyMethod = "close")
    public Connection egressConnection(SequencerProperties properties) throws IOException, InterruptedException {
        return Nats.connect(properties.getEgress().getNats().getUrl());
    }

    /** Management handle used once at startup to ensure the allocator's KV bucket exists. */
    @Bean
    public KeyValueManagement keyValueManagement(Connection egressConnection) throws IOException {
        return egressConnection.keyValueManagement();
    }

    /** Fabric8 client backing Kubernetes Lease-based leader election (spec §10). */
    @Bean(destroyMethod = "close")
    public KubernetesClient kubernetesClient() {
        return new KubernetesClientBuilder().build();
    }

    /** Single-writer enforcement via a Kubernetes Lease; also the allocator's fencing check. */
    @Bean
    public LeaderElection leaderElection(KubernetesClient kubernetesClient, SequencerProperties properties) {
        return new LeaderElection(kubernetesClient, properties);
    }

    /** High-water-mark block leasing over the egress connection's KV bucket (spec §6). */
    @Bean
    public SequenceAllocator sequenceAllocator(Connection egressConnection, SequencerProperties properties,
                                                LeaderElection leaderElection, SequencerMetrics sequencerMetrics) {
        return new SequenceAllocator(egressConnection, properties, leaderElection, sequencerMetrics);
    }

    /** Hot-path stamping logic: offsets and template rules compiled out of config once (spec §3). */
    @Bean
    public SequenceStamper sequenceStamper(SequencerProperties properties, OffsetEpochNanoClock offsetEpochNanoClock,
                                            SequencerMetrics sequencerMetrics) {
        return new SequenceStamper(properties, offsetEpochNanoClock, sequencerMetrics);
    }

    /**
     * Egress destination selected by {@code sequencer.egress.type}, optionally wrapped by the
     * {@link BatchingDestination} decorator per {@code sequencer.egress.batching.enabled}
     * (spec §7, §8). {@code libs/nats-egress} carries no Spring dependency, so this bean method
     * maps {@link SequencerProperties} onto its lean {@link EgressConfig}/{@link BatchingConfig}
     * records (design §3.2).
     */
    @Bean
    public DestinationChannel destinationChannel(Connection egressConnection, SequencerProperties properties,
                                                  SequencerMetrics sequencerMetrics,
                                                  OffsetEpochNanoClock offsetEpochNanoClock) throws IOException {
        SequencerProperties.Egress egress = properties.getEgress();
        EgressConfig egressConfig = new EgressConfig(
                egress.getNats().getSubject(),
                "block".equalsIgnoreCase(egress.getBackpressure()),
                egress.getMaxStallMs(),
                egress.getJetstream().getMaxInFlight());

        DestinationChannel raw = "core".equalsIgnoreCase(egress.getType())
                ? new CoreNatsDestination(egressConnection, egressConfig, sequencerMetrics)
                : new JetStreamDestination(egressConnection.jetStream(), egressConfig, sequencerMetrics);

        if (egress.getBatching().isEnabled()) {
            SequencerProperties.Stamping stamping = properties.getStamping();
            BatchingConfig batchingConfig = new BatchingConfig(
                    stamping.getSchemaId(),
                    stamping.getSequenceIdOffset(),
                    stamping.getTemplateIdOffset(),
                    stamping.getEventEnrichment().getBoundaryTemplateId(),
                    egress.getBatching().isFlushOnEventBoundary(),
                    egress.getBatching().getMaxMessages(),
                    egress.getBatching().getMaxBytes(),
                    egress.getBatching().getMaxLingerMicros());
            return new BatchingDestination(raw, batchingConfig, sequencerMetrics, offsetEpochNanoClock);
        }
        return raw;
    }

    /** NATS ingress selected by {@code sequencer.ingress.nats.mode} (spec §5). */
    @Bean
    public IngressChannel ingressChannel(Connection ingressConnection, SequencerProperties properties,
                                          DestinationChannel destinationChannel) {
        return new NatsSequencerIngress(ingressConnection, properties, destinationChannel);
    }

    /** Builds the sequencer's own high-water-mark heartbeat message (spec §8). */
    @Bean
    public HeartbeatEmitter heartbeatEmitter(SequencerProperties properties) {
        return new HeartbeatEmitter(properties);
    }

    /**
     * The {@link org.springframework.context.SmartLifecycle} orchestrator: startup ordering,
     * single-writer stamp-allocate-publish loop, and graceful shutdown (spec §10).
     */
    @Bean
    public SequencerPipeline sequencerPipeline(IngressChannel ingressChannel, DestinationChannel destinationChannel,
                                                SequenceStamper sequenceStamper, SequenceAllocator sequenceAllocator,
                                                LeaderElection leaderElection, HeartbeatEmitter heartbeatEmitter,
                                                SequencerMetrics sequencerMetrics, KeyValueManagement keyValueManagement,
                                                Connection ingressConnection, Connection egressConnection,
                                                SequencerProperties properties) {
        return new SequencerPipeline(ingressChannel, destinationChannel, sequenceStamper, sequenceAllocator,
                leaderElection, heartbeatEmitter, sequencerMetrics, keyValueManagement, ingressConnection,
                egressConnection, properties);
    }

    /** Readiness: ingress/egress connected, leadership held, schemaId guard passed (spec §10, §13). */
    @Bean
    public SequencerHealthIndicator sequencerHealthIndicator(SequencerPipeline sequencerPipeline,
                                                               Connection ingressConnection,
                                                               Connection egressConnection) {
        return new SequencerHealthIndicator(sequencerPipeline, ingressConnection, egressConnection);
    }
}
