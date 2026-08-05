package gcm.md.natsbridge.bridge;

import gcm.md.natsbridge.config.NatsBridgeProperties;
import gcm.md.natsbridge.metrics.BridgeMetrics;
import gcm.md.sequencer.egress.DestinationChannel;
import io.aeron.Aeron;
import io.aeron.archive.client.AeronArchive;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

import java.nio.ByteOrder;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Exercises {@link BridgePipeline#onFragment} — the actual skip/bridge/gap/checkpoint decision
 * per design §9 — directly and without any live Aeron dependency. The replay-merge/live-poll
 * driving loop in {@link BridgePipeline#start} needs a live Archive/cluster and is out of scope
 * here; see the class Javadoc's confidence note.
 */
class BridgePipelineTest {

    private static final ByteOrder LE = ByteOrder.LITTLE_ENDIAN;
    private static final int SEQUENCE_ID_OFFSET = 8;

    private static UnsafeBuffer message(long sequenceId) {
        UnsafeBuffer buffer = new UnsafeBuffer(new byte[64]);
        buffer.putLong(SEQUENCE_ID_OFFSET, sequenceId, LE);
        return buffer;
    }

    private BridgePipeline newPipeline(long checkpointValue, DestinationChannel destination, BridgeMetrics metrics) {
        NatsBridgeProperties properties = new NatsBridgeProperties();
        properties.getNats().setCheckpointIntervalMessages(1000); // avoid checkpoint writes interfering with assertions unless a test wants them

        BridgeCheckpoint checkpoint = mock(BridgeCheckpoint.class);
        when(checkpoint.read()).thenReturn(checkpointValue);

        return new BridgePipeline(mock(Aeron.class), mock(AeronArchive.class), properties, destination, checkpoint, metrics);
    }

    @Test
    void bridgesANormalMessageAndUpdatesMetrics() {
        DestinationChannel destination = mock(DestinationChannel.class);
        BridgeMetrics metrics = mock(BridgeMetrics.class);
        BridgePipeline pipeline = newPipeline(0L, destination, metrics);

        DirectBuffer message = message(1L);
        pipeline.onFragment(message, 0, message.capacity());

        verify(destination).publish(message, 0, message.capacity());
        verify(metrics).onMessageBridged(1L);
        verify(metrics, never()).onGapDetected(anyInt());
    }

    @Test
    void skipsAMessageAtOrBeforeTheCheckpointWithoutPublishing() {
        DestinationChannel destination = mock(DestinationChannel.class);
        BridgeMetrics metrics = mock(BridgeMetrics.class);
        BridgePipeline pipeline = newPipeline(100L, destination, metrics);

        DirectBuffer message = message(50L);
        pipeline.onFragment(message, 0, message.capacity());

        verify(destination, never()).publish(any(), anyInt(), anyInt());
        verify(metrics, never()).onMessageBridged(50L);
    }

    @Test
    void detectsAndCountsAGapButStillBridgesTheMessage() {
        DestinationChannel destination = mock(DestinationChannel.class);
        BridgeMetrics metrics = mock(BridgeMetrics.class);
        BridgePipeline pipeline = newPipeline(0L, destination, metrics);

        pipeline.onFragment(message(1L), 0, 64);
        DirectBuffer gapped = message(5L); // 2, 3, 4 missing
        pipeline.onFragment(gapped, 0, gapped.capacity());

        verify(metrics).onGapDetected(3L);
        verify(destination).publish(gapped, 0, gapped.capacity()); // still bridged despite the gap
        verify(metrics).onMessageBridged(5L);
    }

    @Test
    void checkpointsOnlyEveryConfiguredIntervalOfMessages() {
        DestinationChannel destination = mock(DestinationChannel.class);
        BridgeMetrics metrics = mock(BridgeMetrics.class);

        NatsBridgeProperties properties = new NatsBridgeProperties();
        properties.getNats().setCheckpointIntervalMessages(3);
        BridgeCheckpoint checkpoint = mock(BridgeCheckpoint.class);
        when(checkpoint.read()).thenReturn(0L);

        BridgePipeline pipeline = new BridgePipeline(mock(Aeron.class), mock(AeronArchive.class), properties,
                destination, checkpoint, metrics);

        pipeline.onFragment(message(1L), 0, 64);
        pipeline.onFragment(message(2L), 0, 64);
        verify(checkpoint, never()).write(anyLong());
        pipeline.onFragment(message(3L), 0, 64);
        verify(checkpoint, times(1)).write(3L);

        pipeline.onFragment(message(4L), 0, 64);
        pipeline.onFragment(message(5L), 0, 64);
        verify(checkpoint, times(1)).write(anyLong()); // still only one write so far
        pipeline.onFragment(message(6L), 0, 64);
        verify(checkpoint, times(1)).write(6L);
    }
}
