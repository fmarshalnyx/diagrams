package gcm.md.sequencer.cluster;

import io.aeron.Aeron;
import io.aeron.Image;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.RecordingDescriptorConsumer;
import io.aeron.logbuffer.FragmentHandler;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;
import org.mockito.invocation.InvocationOnMock;

import java.nio.ByteOrder;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Exercises {@link ArchiveRecordingTailQuery}'s control flow against mocked Archive/Aeron
 * collaborators. This proves the class's own arithmetic and sequencing are internally
 * consistent; it cannot validate real Aeron Archive wire semantics (recording matching, replay
 * session bootstrapping) — see the class Javadoc's confidence note. A live-Archive proof is
 * {@code services/sequencer-aeron/integration-tests}' job (design §12.2).
 */
class ArchiveRecordingTailQueryTest {

    private static final ByteOrder LE = ByteOrder.LITTLE_ENDIAN;
    private static final long RECORDING_ID = 7L;
    private static final long START_POSITION = 0L;
    private static final long STOP_POSITION = 200L;
    private static final int REPLAY_SESSION_ID = 99;

    @Test
    void returnsNoSuppressionWhenNoMatchingRecordingExists() {
        AeronArchive archive = mock(AeronArchive.class);
        when(archive.findLastMatchingRecording(eq(0L), anyString(), anyInt(), anyInt()))
                .thenReturn((long) Aeron.NULL_VALUE);

        ArchiveRecordingTailQuery query = new ArchiveRecordingTailQuery(archive, mock(Aeron.class),
                AeronEgressConfig.localDefaults());

        assertThat(query.lastPublishedSequenceId()).isEqualTo(SuppressionGate.NO_SUPPRESSION);
    }

    @Test
    void returnsNoSuppressionWhenTheMatchingRecordingIsEmpty() {
        AeronArchive archive = mock(AeronArchive.class);
        when(archive.findLastMatchingRecording(eq(0L), anyString(), anyInt(), anyInt())).thenReturn(RECORDING_ID);
        stubEmptyRecordingDescriptor(archive);

        ArchiveRecordingTailQuery query = new ArchiveRecordingTailQuery(archive, mock(Aeron.class),
                AeronEgressConfig.localDefaults());

        assertThat(query.lastPublishedSequenceId()).isEqualTo(SuppressionGate.NO_SUPPRESSION);
    }

    @Test
    void replaysTheRecordingAndReturnsTheLastFragmentsSequenceId() {
        AeronArchive archive = mock(AeronArchive.class);
        Aeron aeron = mock(Aeron.class);
        AeronEgressConfig config = AeronEgressConfig.localDefaults();

        when(archive.findLastMatchingRecording(eq(0L), eq(config.egressChannel()), eq(config.egressStreamId()),
                anyInt())).thenReturn(RECORDING_ID);
        stubNonEmptyRecordingDescriptor(archive);
        when(archive.startReplay(eq(RECORDING_ID), eq(START_POSITION), eq(STOP_POSITION - START_POSITION),
                anyString(), anyInt())).thenReturn((long) REPLAY_SESSION_ID);

        UnsafeBuffer message1 = message(1001L, config);
        UnsafeBuffer message2 = message(1002L, config); // the later, "last" message

        Subscription subscription = mock(Subscription.class);
        when(aeron.addSubscription(anyString(), anyInt())).thenReturn(subscription);
        Image image = fakeImageDelivering(subscription, REPLAY_SESSION_ID, STOP_POSITION, message1, message2);

        ArchiveRecordingTailQuery query = new ArchiveRecordingTailQuery(archive, aeron, config);

        assertThat(query.lastPublishedSequenceId()).isEqualTo(1002L);
    }

    private static UnsafeBuffer message(long sequenceId, AeronEgressConfig config) {
        UnsafeBuffer buffer = new UnsafeBuffer(new byte[64]);
        buffer.putLong(config.sequenceIdOffset(), sequenceId, LE);
        return buffer;
    }

    private static void stubEmptyRecordingDescriptor(AeronArchive archive) {
        when(archive.listRecording(eq(RECORDING_ID), any(RecordingDescriptorConsumer.class)))
                .thenAnswer((InvocationOnMock inv) -> {
                    RecordingDescriptorConsumer consumer = inv.getArgument(1);
                    consumer.onRecordingDescriptor(0, 0, RECORDING_ID, 0, 0, 50L, 50L, 0, 0, 0, 0, 0, 0, "", "", "");
                    return 1;
                });
    }

    private static void stubNonEmptyRecordingDescriptor(AeronArchive archive) {
        when(archive.listRecording(eq(RECORDING_ID), any(RecordingDescriptorConsumer.class)))
                .thenAnswer((InvocationOnMock inv) -> {
                    RecordingDescriptorConsumer consumer = inv.getArgument(1);
                    consumer.onRecordingDescriptor(0, 0, RECORDING_ID, 0, 0, START_POSITION, STOP_POSITION,
                            0, 0, 0, 0, 0, 0, "", "", "");
                    return 1;
                });
    }

    /** A fake {@link Image} that delivers each given message as one fragment, then reports end-of-stream. */
    private static Image fakeImageDelivering(Subscription subscription, int sessionId, long stopPosition,
                                              UnsafeBuffer... messages) {
        Image image = mock(Image.class);
        when(subscription.imageBySessionId(sessionId)).thenReturn(image);
        when(image.isClosed()).thenReturn(false);

        int[] index = {0};
        long[] position = {0L};
        when(image.position()).thenAnswer(inv -> position[0]);
        when(image.isEndOfStream()).thenAnswer(inv -> index[0] >= messages.length);
        when(image.poll(any(FragmentHandler.class), anyInt())).thenAnswer((InvocationOnMock inv) -> {
            if (index[0] >= messages.length) {
                return 0;
            }
            FragmentHandler handler = inv.getArgument(0);
            UnsafeBuffer message = messages[index[0]];
            handler.onFragment(message, 0, message.capacity(), null);
            index[0]++;
            position[0] = index[0] >= messages.length ? stopPosition : position[0] + message.capacity();
            return 1;
        });
        return image;
    }
}
