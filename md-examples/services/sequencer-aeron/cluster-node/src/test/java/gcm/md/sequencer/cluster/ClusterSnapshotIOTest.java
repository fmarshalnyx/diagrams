package gcm.md.sequencer.cluster;

import io.aeron.ExclusivePublication;
import io.aeron.Image;
import io.aeron.logbuffer.FragmentHandler;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.invocation.InvocationOnMock;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ClusterSnapshotIOTest {

    @Test
    void writerThenReaderRoundTripsPrimitivesInOrder() {
        ClusterSnapshotIO.Writer writer = new ClusterSnapshotIO.Writer();
        writer.putLong(42L);
        writer.putInt(7);
        writer.putLong(-1L);

        ExclusivePublication publication = mock(ExclusivePublication.class);
        ArgumentCaptor<DirectBuffer> bufferCaptor = ArgumentCaptor.forClass(DirectBuffer.class);
        ArgumentCaptor<Integer> lengthCaptor = ArgumentCaptor.forClass(Integer.class);
        when(publication.offer(bufferCaptor.capture(), anyInt(), lengthCaptor.capture())).thenReturn(1L);

        writer.flushTo(publication);

        // Feed the offered bytes through a fake Image that delivers them as one fragment.
        DirectBuffer offered = bufferCaptor.getValue();
        int length = lengthCaptor.getValue();
        UnsafeBuffer copy = new UnsafeBuffer(new byte[length]);
        copy.putBytes(0, offered, 0, length);

        Image image = fakeImageDeliveringOneFragment(copy, length);
        ClusterSnapshotIO.Reader reader = ClusterSnapshotIO.Reader.drain(image);

        assertThat(reader.nextLong()).isEqualTo(42L);
        assertThat(reader.nextInt()).isEqualTo(7);
        assertThat(reader.nextLong()).isEqualTo(-1L);
    }

    /** A minimal {@link Image} stand-in: delivers one fragment on the first poll, then reports end-of-stream. */
    private static Image fakeImageDeliveringOneFragment(DirectBuffer fragment, int length) {
        Image image = mock(Image.class);
        boolean[] delivered = {false};
        when(image.isEndOfStream()).thenAnswer(inv -> delivered[0]);
        when(image.isClosed()).thenReturn(false);
        when(image.poll(any(FragmentHandler.class), anyInt())).thenAnswer((InvocationOnMock inv) -> {
            if (delivered[0]) {
                return 0;
            }
            FragmentHandler handler = inv.getArgument(0);
            handler.onFragment(fragment, 0, length, null);
            delivered[0] = true;
            return 1;
        });
        return image;
    }
}
