package gcm.md.mockupstreamsource.generate;

import gcm.md.mockupstreamsource.config.MockUpstreamSourceProperties;
import gcm.md.mockupstreamsource.metrics.MockUpstreamSourceMetrics;
import gcm.md.mockupstreamsource.verify.EgressConsumer;
import gcm.md.mockupstreamsource.verify.SequenceVerifier;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.nats.client.JetStream;
import org.junit.jupiter.api.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TrafficGeneratorTest {

    private TrafficGenerator newGenerator(MessagePlanner planner) {
        MockUpstreamSourceProperties properties = new MockUpstreamSourceProperties();
        SequenceVerifier verifier = new SequenceVerifier();
        EgressConsumer egressConsumer = new EgressConsumer(false, verifier);
        MockUpstreamSourceMetrics metrics = new MockUpstreamSourceMetrics(new SimpleMeterRegistry(), verifier,
                egressConsumer);
        return new TrafficGenerator(null, properties, planner, new MessageEncoder(), metrics);
    }

    @Test
    void publishesOnceOnAPlainPublishDecision() throws Exception {
        MessagePlanner planner = new MessagePlanner(0.0, 0.0, 1L); // always PUBLISH
        TrafficGenerator generator = newGenerator(planner);
        JetStream jetStream = mock(JetStream.class);

        generator.emitOne(jetStream);

        verify(jetStream, times(1)).publish(anyString(), any(byte[].class));
    }

    @Test
    void neverPublishesOnASkipDecision() throws Exception {
        MessagePlanner planner = new MessagePlanner(1.0, 0.0, 1L); // always SKIP
        TrafficGenerator generator = newGenerator(planner);
        JetStream jetStream = mock(JetStream.class);

        generator.emitOne(jetStream);

        verify(jetStream, never()).publish(anyString(), any(byte[].class));
    }

    @Test
    void publishesTwiceOnAPublishThenDuplicateDecision() throws Exception {
        MessagePlanner planner = new MessagePlanner(0.0, 1.0, 1L); // always PUBLISH_THEN_DUPLICATE
        TrafficGenerator generator = newGenerator(planner);
        JetStream jetStream = mock(JetStream.class);

        generator.emitOne(jetStream);

        verify(jetStream, times(2)).publish(anyString(), any(byte[].class));
    }

    @Test
    void skipsTheDuplicatePublishWhenTheFirstOneFails() throws Exception {
        MessagePlanner planner = new MessagePlanner(0.0, 1.0, 1L); // always PUBLISH_THEN_DUPLICATE
        TrafficGenerator generator = newGenerator(planner);
        JetStream jetStream = mock(JetStream.class);
        when(jetStream.publish(anyString(), any(byte[].class))).thenThrow(new RuntimeException("boom"));

        generator.emitOne(jetStream);

        // Only the first (failed) attempt — never a second publish for a message that never went out.
        verify(jetStream, times(1)).publish(anyString(), any(byte[].class));
    }
}
