package gcm.md.sequencer.clusterclient;

import io.aeron.Publication;
import io.aeron.cluster.client.AeronCluster;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Design §7 acceptance: "unit tests against a local single-member cluster: offer, induced
 * back-pressure, reconnect-after-drop, credential rejection." A live cluster is unnecessary for
 * these — {@link AeronCluster} is mocked and the connect strategy is injected, since the
 * behavior under test is entirely {@link ClusterIngressClient}'s own retry/backoff/reconnect
 * logic, not Aeron's wire protocol.
 */
class ClusterIngressClientTest {

    private static final IngressClientConfig FAST_RETRY_CONFIG = new IngressClientConfig(3, 1_000L);
    private static final UnsafeBuffer MESSAGE = new UnsafeBuffer(new byte[16]);

    @Test
    void offerReturnsTheSuccessPositionOnTheHappyPath() {
        AeronCluster cluster = mock(AeronCluster.class);
        when(cluster.offer(MESSAGE, 0, MESSAGE.capacity())).thenReturn(42L);

        ClusterIngressClient client = new ClusterIngressClient(mock(AeronCluster.Context.class),
                FAST_RETRY_CONFIG, ctx -> cluster);

        assertThat(client.offer(MESSAGE, 0, MESSAGE.capacity())).isEqualTo(42L);
        verify(cluster).pollEgress(); // polled once per offer to process control events
    }

    @Test
    void inducedBackPressureIsRetriedUpToTheBoundThenSurfacedToTheCaller() {
        AeronCluster cluster = mock(AeronCluster.class);
        when(cluster.offer(MESSAGE, 0, MESSAGE.capacity())).thenReturn(Publication.BACK_PRESSURED);

        ClusterIngressClient client = new ClusterIngressClient(mock(AeronCluster.Context.class),
                FAST_RETRY_CONFIG, ctx -> cluster);

        long result = client.offer(MESSAGE, 0, MESSAGE.capacity());

        assertThat(result).isEqualTo(Publication.BACK_PRESSURED);
        // Never blocks unboundedly: exactly maxBackpressureAttempts offer() calls, then surfaced.
        verify(cluster, times(FAST_RETRY_CONFIG.maxBackpressureAttempts())).offer(MESSAGE, 0, MESSAGE.capacity());
    }

    @Test
    void backPressureThatClearsWithinTheBoundSucceeds() {
        AeronCluster cluster = mock(AeronCluster.class);
        when(cluster.offer(MESSAGE, 0, MESSAGE.capacity()))
                .thenReturn(Publication.BACK_PRESSURED, Publication.BACK_PRESSURED, 100L);

        ClusterIngressClient client = new ClusterIngressClient(mock(AeronCluster.Context.class),
                FAST_RETRY_CONFIG, ctx -> cluster);

        assertThat(client.offer(MESSAGE, 0, MESSAGE.capacity())).isEqualTo(100L);
    }

    @Test
    void reconnectAfterDropEstablishesAFreshSessionOnTheNextOffer() {
        AeronCluster droppedCluster = mock(AeronCluster.class);
        when(droppedCluster.offer(MESSAGE, 0, MESSAGE.capacity())).thenReturn(Publication.NOT_CONNECTED);
        AeronCluster freshCluster = mock(AeronCluster.class);
        when(freshCluster.offer(MESSAGE, 0, MESSAGE.capacity())).thenReturn(1L);

        Deque<AeronCluster> toConnect = new ArrayDeque<>(List.of(droppedCluster, freshCluster));
        Function<AeronCluster.Context, AeronCluster> connector = ctx -> toConnect.removeFirst();

        ClusterIngressClient client = new ClusterIngressClient(mock(AeronCluster.Context.class),
                FAST_RETRY_CONFIG, connector);

        long firstResult = client.offer(MESSAGE, 0, MESSAGE.capacity());
        assertThat(firstResult).isEqualTo(Publication.NOT_CONNECTED); // surfaced immediately, not retried

        long secondResult = client.offer(MESSAGE, 0, MESSAGE.capacity());
        assertThat(secondResult).isEqualTo(1L); // now using the reconnected session
    }

    @Test
    void closedSessionAlsoTriggersReconnect() {
        AeronCluster droppedCluster = mock(AeronCluster.class);
        when(droppedCluster.offer(MESSAGE, 0, MESSAGE.capacity())).thenReturn(Publication.CLOSED);
        AeronCluster freshCluster = mock(AeronCluster.class);
        when(freshCluster.offer(MESSAGE, 0, MESSAGE.capacity())).thenReturn(1L);

        Deque<AeronCluster> toConnect = new ArrayDeque<>(List.of(droppedCluster, freshCluster));
        ClusterIngressClient client = new ClusterIngressClient(mock(AeronCluster.Context.class),
                FAST_RETRY_CONFIG, ctx -> toConnect.removeFirst());

        client.offer(MESSAGE, 0, MESSAGE.capacity());
        assertThat(client.offer(MESSAGE, 0, MESSAGE.capacity())).isEqualTo(1L);
    }

    @Test
    void credentialRejectionAtConnectTimePropagatesFromTheConstructor() {
        Function<AeronCluster.Context, AeronCluster> rejectingConnector = ctx -> {
            throw new io.aeron.cluster.client.ClusterException("credential rejected");
        };

        assertThatThrownBy(() -> new ClusterIngressClient(mock(AeronCluster.Context.class),
                FAST_RETRY_CONFIG, rejectingConnector))
                .isInstanceOf(io.aeron.cluster.client.ClusterException.class);
    }

    @Test
    void maxPositionExceededIsSurfacedImmediatelyWithoutRetryOrReconnect() {
        AeronCluster cluster = mock(AeronCluster.class);
        when(cluster.offer(MESSAGE, 0, MESSAGE.capacity())).thenReturn(Publication.MAX_POSITION_EXCEEDED);

        ClusterIngressClient client = new ClusterIngressClient(mock(AeronCluster.Context.class),
                FAST_RETRY_CONFIG, ctx -> cluster);

        assertThat(client.offer(MESSAGE, 0, MESSAGE.capacity())).isEqualTo(Publication.MAX_POSITION_EXCEEDED);
        verify(cluster, times(1)).offer(MESSAGE, 0, MESSAGE.capacity());
    }
}
