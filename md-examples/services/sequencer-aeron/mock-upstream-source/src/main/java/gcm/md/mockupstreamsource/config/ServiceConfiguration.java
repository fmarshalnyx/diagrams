package gcm.md.mockupstreamsource.config;

import gcm.md.mockupstreamsource.generate.MessageEncoder;
import gcm.md.mockupstreamsource.generate.MessagePlanner;
import gcm.md.mockupstreamsource.generate.TrafficGenerator;
import gcm.md.mockupstreamsource.metrics.MockUpstreamSourceMetrics;
import gcm.md.mockupstreamsource.verify.EgressConsumer;
import gcm.md.mockupstreamsource.verify.SequenceVerifier;
import io.micrometer.core.instrument.MeterRegistry;
import io.nats.client.Connection;
import io.nats.client.JetStream;
import io.nats.client.Nats;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;

/**
 * Sole source of bean definitions for {@code mock-upstream-source}, per project convention: no
 * {@code @Autowired}, no stereotype-annotated business classes — every collaborator is wired
 * explicitly here.
 */
@Configuration
@EnableConfigurationProperties(MockUpstreamSourceProperties.class)
public class ServiceConfiguration {

    /** Single connection used for both generating and verifying (same pattern as sequencer-loadgen). */
    @Bean(destroyMethod = "close")
    public Connection natsConnection(MockUpstreamSourceProperties properties) throws IOException, InterruptedException {
        return Nats.connect(properties.getUrl());
    }

    @Bean
    public MessagePlanner messagePlanner(MockUpstreamSourceProperties properties) {
        return new MessagePlanner(properties.getGapProbability(), properties.getDuplicateProbability(),
                properties.getSeed());
    }

    @Bean
    public MessageEncoder messageEncoder() {
        return new MessageEncoder();
    }

    @Bean
    public SequenceVerifier sequenceVerifier() {
        return new SequenceVerifier();
    }

    /** Used only by {@link #egressConsumer} — a JetStream (not core NATS) subscription, see its Javadoc for why. */
    @Bean
    public JetStream egressJetStream(Connection natsConnection) throws IOException {
        return natsConnection.jetStream();
    }

    /** Subscribed to {@code egress-subject} immediately — verification runs for the service's whole lifetime. */
    @Bean
    public EgressConsumer egressConsumer(Connection natsConnection, JetStream egressJetStream,
                                          SequenceVerifier sequenceVerifier,
                                          MockUpstreamSourceProperties properties) throws Exception {
        EgressConsumer consumer = new EgressConsumer(properties.isBatched(), sequenceVerifier);
        consumer.subscribe(natsConnection, egressJetStream, properties.getEgressSubject());
        return consumer;
    }

    @Bean
    public MockUpstreamSourceMetrics mockUpstreamSourceMetrics(MeterRegistry registry, SequenceVerifier sequenceVerifier,
                                                                 EgressConsumer egressConsumer) {
        return new MockUpstreamSourceMetrics(registry, sequenceVerifier, egressConsumer);
    }

    @Bean
    public TrafficGenerator trafficGenerator(Connection natsConnection, MockUpstreamSourceProperties properties,
                                              MessagePlanner messagePlanner, MessageEncoder messageEncoder,
                                              MockUpstreamSourceMetrics metrics) {
        return new TrafficGenerator(natsConnection, properties, messagePlanner, messageEncoder, metrics);
    }
}
