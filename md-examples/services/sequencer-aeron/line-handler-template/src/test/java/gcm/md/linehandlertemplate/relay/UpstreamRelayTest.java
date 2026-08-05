package gcm.md.linehandlertemplate.relay;

import gcm.md.linehandlertemplate.config.LineHandlerProperties;
import gcm.md.linehandlertemplate.metrics.LineHandlerMetrics;
import gcm.md.sequencer.ingress.IngressTransport;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.nats.client.Message;
import io.nats.client.impl.NatsJetStreamMetaData;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * {@link UpstreamRelay#onFetched}/{@link UpstreamRelay#offerWithRetry} are the direct proof of
 * this module's central claim: a message is acked if and only if it was successfully offered,
 * with no handler-side bookkeeping — see the class Javadoc's crash-recovery contract.
 */
class UpstreamRelayTest {

    private static final int STAMP_OFFSET = 64;

    private LineHandlerProperties properties(long retryParkNanos) {
        LineHandlerProperties properties = new LineHandlerProperties();
        properties.getStamping().setSourceSeqNumOffset(STAMP_OFFSET);
        properties.getOffer().setRetryParkNanos(retryParkNanos);
        properties.getOffer().setWarnEveryNAttempts(2);
        return properties;
    }

    private UpstreamRelay newRelay(IngressTransport transport, long retryParkNanos) {
        LineHandlerMetrics metrics = new LineHandlerMetrics(new SimpleMeterRegistry());
        SourceSeqNumStamper stamper = new SourceSeqNumStamper(STAMP_OFFSET);
        return new UpstreamRelay(null, properties(retryParkNanos), transport, stamper, metrics);
    }

    private Message mockMessage(long streamSequence, byte[] data) {
        Message message = mock(Message.class);
        NatsJetStreamMetaData metaData = mock(NatsJetStreamMetaData.class);
        when(metaData.streamSequence()).thenReturn(streamSequence);
        when(message.metaData()).thenReturn(metaData);
        when(message.getData()).thenReturn(data);
        return message;
    }

    @Test
    void offerWithRetryRetriesExactlyUntilTheTransportSucceeds() {
        IngressTransport transport = mock(IngressTransport.class);
        when(transport.offer(any(), anyInt(), anyInt())).thenReturn(-1L, -1L, -1L, 7L);
        UpstreamRelay relay = newRelay(transport, 1_000L); // 1us idle, fast test
        relay.markRunningForTest();

        UnsafeBuffer buffer = new UnsafeBuffer(new byte[STAMP_OFFSET + 4]);
        boolean result = relay.offerWithRetry(buffer, 0, buffer.capacity());

        assertThat(result).isTrue();
        verify(transport, times(4)).offer(any(), anyInt(), anyInt());
    }

    @Test
    void offerWithRetryStopsAndReturnsFalseOnceKeepRunningTurnsFalse() {
        IngressTransport transport = mock(IngressTransport.class);
        when(transport.offer(any(), anyInt(), anyInt())).thenReturn(-1L); // perpetual backpressure
        UpstreamRelay relay = newRelay(transport, 1_000L);

        AtomicInteger callsRemaining = new AtomicInteger(3);
        UnsafeBuffer buffer = new UnsafeBuffer(new byte[STAMP_OFFSET + 4]);
        boolean result = relay.offerWithRetry(buffer, 0, buffer.capacity(),
                () -> callsRemaining.getAndDecrement() > 0);

        assertThat(result).isFalse();
        verify(transport, times(3)).offer(any(), anyInt(), anyInt());
    }

    @Test
    void onFetchedAcksOnlyAfterASuccessfulOffer() {
        IngressTransport transport = mock(IngressTransport.class);
        when(transport.offer(any(), anyInt(), anyInt())).thenReturn(5L);
        UpstreamRelay relay = newRelay(transport, 1_000L);
        relay.markRunningForTest();

        Message message = mockMessage(42L, new byte[STAMP_OFFSET + 4]);
        relay.onFetched(message);

        verify(message).ack();
    }

    @Test
    void onFetchedNeverAcksWhenShutdownInterruptsTheRetryLoop() {
        IngressTransport transport = mock(IngressTransport.class);
        UpstreamRelay relay = newRelay(transport, 1_000L);
        relay.markRunningForTest();
        // Perpetual backpressure, but flip running off after a couple of attempts so the retry
        // loop (bound to relay's own `running` field via the 3-arg onFetched path) terminates.
        AtomicInteger calls = new AtomicInteger();
        when(transport.offer(any(), anyInt(), anyInt())).thenAnswer(invocation -> {
            if (calls.incrementAndGet() >= 2) {
                relay.stop();
            }
            return -1L;
        });

        Message message = mockMessage(42L, new byte[STAMP_OFFSET + 4]);
        relay.onFetched(message);

        verify(message, never()).ack();
    }

    @Test
    void stampsTheMessageBufferWithTheUpstreamStreamSequenceNumber() {
        IngressTransport transport = mock(IngressTransport.class);
        when(transport.offer(any(), anyInt(), anyInt())).thenReturn(1L);
        UpstreamRelay relay = newRelay(transport, 1_000L);
        relay.markRunningForTest();

        byte[] data = new byte[STAMP_OFFSET + 4];
        Message message = mockMessage(123L, data);
        relay.onFetched(message);

        UnsafeBuffer readBack = new UnsafeBuffer(data);
        assertThat(readBack.getInt(STAMP_OFFSET, java.nio.ByteOrder.LITTLE_ENDIAN)).isEqualTo(123);
    }
}
