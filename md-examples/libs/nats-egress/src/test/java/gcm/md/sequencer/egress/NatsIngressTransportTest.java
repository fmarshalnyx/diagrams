package gcm.md.sequencer.egress;

import gcm.md.sequencer.ingress.IngressTransport;
import io.nats.client.JetStream;
import io.nats.client.api.PublishAck;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * {@link IngressTransport}'s contract ("negative on backpressure, never block/drop") applied to
 * the NATS side — the counterpart to {@code ClusterIngressClient}'s own bounded-retry behavior,
 * verified here without a live NATS connection.
 */
class NatsIngressTransportTest {

    private static final byte[] PAYLOAD = {1, 2, 3, 4};

    private static final class RecordingEgressMetrics implements EgressMetrics {
        int lastInflightWindow;
        int publishFailures;

        @Override
        public void incrementDropped() {
        }

        @Override
        public void incrementPublishFailures() {
            publishFailures++;
        }

        @Override
        public void recordBackpressureStall(long nanos) {
        }

        @Override
        public void setInflightWindow(int inflight) {
            lastInflightWindow = inflight;
        }

        @Override
        public void onBatchFlushed(int messageCount) {
        }
    }

    @Test
    void acceptsAnOfferWhenWindowHasRoom() {
        JetStream jetStream = mock(JetStream.class);
        when(jetStream.publishAsync(eq("MD_RAW"), any(byte[].class))).thenReturn(new CompletableFuture<>());
        RecordingEgressMetrics metrics = new RecordingEgressMetrics();
        NatsIngressTransport transport =
                new NatsIngressTransport(jetStream, new NatsIngressConfig("MD_RAW", 2), metrics);

        long result = transport.offer(new UnsafeBuffer(PAYLOAD), 0, PAYLOAD.length);

        assertThat(result).isGreaterThanOrEqualTo(0);
        verify(jetStream).publishAsync(eq("MD_RAW"), any(byte[].class));
        assertThat(metrics.lastInflightWindow).isEqualTo(1);
    }

    @Test
    void surfacesBackpressureOnceWindowIsFullInsteadOfBlockingOrDropping() {
        JetStream jetStream = mock(JetStream.class);
        when(jetStream.publishAsync(eq("MD_RAW"), any(byte[].class))).thenReturn(new CompletableFuture<>());
        NatsIngressTransport transport =
                new NatsIngressTransport(jetStream, new NatsIngressConfig("MD_RAW", 1), new RecordingEgressMetrics());

        long first = transport.offer(new UnsafeBuffer(PAYLOAD), 0, PAYLOAD.length);
        long second = transport.offer(new UnsafeBuffer(PAYLOAD), 0, PAYLOAD.length);

        assertThat(first).isGreaterThanOrEqualTo(0);
        assertThat(second).isNegative();
        // Only the first offer should have actually reached JetStream — the second was rejected
        // before ever attempting to publish, per the "surface, don't block or drop" contract.
        verify(jetStream, times(1)).publishAsync(eq("MD_RAW"), any(byte[].class));
    }

    @Test
    void windowFreesUpOnceAnInFlightPublishCompletes() {
        JetStream jetStream = mock(JetStream.class);
        CompletableFuture<PublishAck> pending = new CompletableFuture<>();
        when(jetStream.publishAsync(eq("MD_RAW"), any(byte[].class))).thenReturn(pending);
        RecordingEgressMetrics metrics = new RecordingEgressMetrics();
        NatsIngressTransport transport =
                new NatsIngressTransport(jetStream, new NatsIngressConfig("MD_RAW", 1), metrics);

        transport.offer(new UnsafeBuffer(PAYLOAD), 0, PAYLOAD.length);
        assertThat(transport.offer(new UnsafeBuffer(PAYLOAD), 0, PAYLOAD.length)).isNegative();

        pending.complete(mock(PublishAck.class));

        assertThat(metrics.lastInflightWindow).isZero();
        assertThat(transport.offer(new UnsafeBuffer(PAYLOAD), 0, PAYLOAD.length)).isGreaterThanOrEqualTo(0);
    }

    @Test
    void countsAFailedPublishAsAFailureNotABlockedRetry() {
        JetStream jetStream = mock(JetStream.class);
        CompletableFuture<PublishAck> pending = new CompletableFuture<>();
        when(jetStream.publishAsync(eq("MD_RAW"), any(byte[].class))).thenReturn(pending);
        RecordingEgressMetrics metrics = new RecordingEgressMetrics();
        NatsIngressTransport transport =
                new NatsIngressTransport(jetStream, new NatsIngressConfig("MD_RAW", 1), metrics);

        transport.offer(new UnsafeBuffer(PAYLOAD), 0, PAYLOAD.length);
        pending.completeExceptionally(new RuntimeException("boom"));

        assertThat(metrics.publishFailures).isEqualTo(1);
        assertThat(metrics.lastInflightWindow).isZero();
    }
}
