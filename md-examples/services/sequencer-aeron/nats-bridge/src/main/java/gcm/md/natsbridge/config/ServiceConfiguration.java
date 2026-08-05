package gcm.md.natsbridge.config;

import gcm.md.natsbridge.bridge.BridgeCheckpoint;
import gcm.md.natsbridge.bridge.BridgePipeline;
import gcm.md.natsbridge.bridge.LeaderArchiveConnector;
import gcm.md.natsbridge.metrics.BridgeMetrics;
import gcm.md.sequencer.egress.BatchingConfig;
import gcm.md.sequencer.egress.BatchingDestination;
import gcm.md.sequencer.egress.DestinationChannel;
import gcm.md.sequencer.egress.EgressConfig;
import gcm.md.sequencer.egress.JetStreamDestination;
import io.aeron.Aeron;
import io.aeron.archive.client.AeronArchive;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.micrometer.core.instrument.MeterRegistry;
import io.nats.client.Connection;
import io.nats.client.JetStream;
import io.nats.client.KeyValue;
import io.nats.client.KeyValueManagement;
import io.nats.client.Nats;
import io.nats.client.api.KeyValueConfiguration;
import org.agrona.concurrent.OffsetEpochNanoClock;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * Sole source of bean definitions for {@code nats-bridge}, per project convention: no
 * {@code @Autowired}, no stereotype-annotated business classes — every collaborator is wired
 * explicitly here.
 */
@Configuration
@EnableConfigurationProperties(NatsBridgeProperties.class)
public class ServiceConfiguration {

    /** NATS connection republishing to {@code MD_SEQUENCED} and holding the checkpoint KV bucket. */
    @Bean(destroyMethod = "close")
    public Connection natsConnection(NatsBridgeProperties properties) throws IOException, InterruptedException {
        return Nats.connect(properties.getNats().getUrl());
    }

    /** Ensures the checkpoint KV bucket exists, then returns a handle to it. */
    @Bean
    public KeyValue checkpointKeyValue(Connection natsConnection, NatsBridgeProperties properties) throws IOException {
        NatsBridgeProperties.Nats config = properties.getNats();
        KeyValueManagement kvm = natsConnection.keyValueManagement();
        try {
            kvm.create(KeyValueConfiguration.builder().name(config.getCheckpointKvBucket()).build());
        } catch (Exception alreadyExists) {
            // Bucket already exists from a prior run — expected on every restart but the first.
        }
        return natsConnection.keyValue(config.getCheckpointKvBucket());
    }

    /** Wraps the checkpoint KV handle (design §9: "stateless-restartable, never authoritative"). */
    @Bean
    public BridgeCheckpoint bridgeCheckpoint(KeyValue checkpointKeyValue, NatsBridgeProperties properties) {
        return new BridgeCheckpoint(checkpointKeyValue, properties.getNats().getCheckpointKvKey());
    }

    /** Bridge counters/gauges, reported through {@code EgressMetrics} for {@code libs/nats-egress}. */
    @Bean
    public BridgeMetrics bridgeMetrics(MeterRegistry meterRegistry) {
        return new BridgeMetrics(meterRegistry);
    }

    /** Calibrated clock used for {@code MessageBatch.batchTimestamp} (design §8, phase-1 §8). */
    @Bean
    public OffsetEpochNanoClock offsetEpochNanoClock() {
        return new OffsetEpochNanoClock();
    }

    /**
     * Destination republishing to {@code MD_SEQUENCED}, optionally batched (design §9: same
     * {@code libs/nats-egress} source phase-1 compiles against — batching behavior is identical
     * by construction, not re-implemented here).
     */
    @Bean
    public DestinationChannel destinationChannel(Connection natsConnection, NatsBridgeProperties properties,
                                                  BridgeMetrics bridgeMetrics,
                                                  OffsetEpochNanoClock offsetEpochNanoClock) throws IOException {
        NatsBridgeProperties.Nats natsConfig = properties.getNats();
        EgressConfig egressConfig = new EgressConfig(natsConfig.getSubject(), true,
                natsConfig.getMaxStallMs(), natsConfig.getJetstreamMaxInFlight());
        JetStream jetStream = natsConnection.jetStream();
        DestinationChannel raw = new JetStreamDestination(jetStream, egressConfig, bridgeMetrics);

        NatsBridgeProperties.Batching batchingProperties = properties.getBatching();
        if (!batchingProperties.isEnabled()) {
            return raw;
        }
        BatchingConfig batchingConfig = new BatchingConfig(
                properties.getStamping().getSchemaId(),
                properties.getStamping().getSequenceIdOffset(),
                batchingProperties.getTemplateIdOffset(),
                batchingProperties.getBoundaryTemplateId(),
                batchingProperties.isFlushOnEventBoundary(),
                batchingProperties.getMaxMessages(),
                batchingProperties.getMaxBytes(),
                batchingProperties.getMaxLingerMicros());
        return new BatchingDestination(raw, batchingConfig, bridgeMetrics, offsetEpochNanoClock);
    }

    /**
     * A lightweight embedded media driver so this process's Aeron client has something to
     * attach to — {@code Aeron.connect()} does not launch one implicitly, it expects one
     * already running at the given {@code aeron.dir}.
     */
    @Bean(destroyMethod = "close")
    public MediaDriver embeddedMediaDriver() {
        MediaDriver.Context context = new MediaDriver.Context()
                .threadingMode(ThreadingMode.SHARED)
                .dirDeleteOnStart(true)
                .dirDeleteOnShutdown(true);
        return MediaDriver.launchEmbedded(context);
    }

    /** External Aeron client connecting to the cluster's egress (not embedded — design §9 runs as its own process). */
    @Bean(destroyMethod = "close")
    public Aeron aeron(MediaDriver embeddedMediaDriver) {
        return Aeron.connect(new Aeron.Context().aeronDirectoryName(embeddedMediaDriver.aeronDirectoryName()));
    }

    @Bean
    public LeaderArchiveConnector leaderArchiveConnector() {
        return new LeaderArchiveConnector();
    }

    /**
     * Archive client used both to find the egress recording and to drive {@code ReplayMerge}
     * (design §9) — connected to whichever cluster member currently has the live recording, not
     * a single fixed endpoint. See {@link LeaderArchiveConnector}'s Javadoc for why a bare
     * headless-Service DNS name (this bean's original implementation) is a real bug, not just a
     * simplification: Archive control connections have no {@code AeronCluster}-style automatic
     * leader-following, and only the leader's own local archive ever records the egress stream.
     */
    @Bean(destroyMethod = "close")
    public AeronArchive aeronArchive(Aeron aeron, NatsBridgeProperties properties,
                                      LeaderArchiveConnector leaderArchiveConnector) {
        NatsBridgeProperties.Cluster clusterConfig = properties.getCluster();
        List<AeronArchive.Context> candidates = Arrays.stream(clusterConfig.getArchiveControlChannels().split(","))
                .map(String::trim)
                .map(channel -> new AeronArchive.Context()
                        .aeron(aeron)
                        .ownsAeronClient(false)
                        .controlRequestChannel(channel)
                        .controlResponseChannel(clusterConfig.getArchiveControlResponseChannel()))
                .toList();
        return leaderArchiveConnector.connectToRecordingOwner(candidates, clusterConfig.getLiveDestination(),
                clusterConfig.getEgressStreamId(), BridgePipeline.EGRESS_SESSION_ID);
    }

    /** The replay-merge-then-live subscription pipeline (design §9). */
    @Bean
    public BridgePipeline bridgePipeline(Aeron aeron, AeronArchive aeronArchive, NatsBridgeProperties properties,
                                          DestinationChannel destinationChannel, BridgeCheckpoint bridgeCheckpoint,
                                          BridgeMetrics bridgeMetrics) {
        return new BridgePipeline(aeron, aeronArchive, properties, destinationChannel, bridgeCheckpoint, bridgeMetrics);
    }
}
