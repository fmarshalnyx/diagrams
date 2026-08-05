package gcm.md.sequencer.cluster;

import gcm.md.sequencer.stamping.SnapshotSink;
import gcm.md.sequencer.stamping.SnapshotSource;
import io.aeron.ExclusivePublication;
import io.aeron.Image;
import io.aeron.Publication;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;

import java.nio.ByteOrder;

/**
 * Bridges {@link gcm.md.sequencer.stamping.StampingEngine}'s transport-agnostic snapshot
 * contract onto Aeron cluster's snapshot mechanism ({@code onTakeSnapshot(ExclusivePublication)}
 * / {@code onStart(Cluster, Image)} with a non-null snapshot image — design §5.1).
 *
 * <p>The engine's full replicated state is small (a counter plus two bounded maps), so both
 * directions buffer the whole snapshot in memory rather than streaming it incrementally.
 */
final class ClusterSnapshotIO {

    private static final ByteOrder LE = ByteOrder.LITTLE_ENDIAN;
    private static final int POLL_FRAGMENT_LIMIT = 10;

    private ClusterSnapshotIO() {
    }

    /** Accumulates {@link SnapshotSink} writes in memory, then offers them as one message. */
    static final class Writer implements SnapshotSink {

        private final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(256);
        private int length;

        @Override
        public void putLong(long value) {
            buffer.putLong(length, value, LE);
            length += Long.BYTES;
        }

        @Override
        public void putInt(int value) {
            buffer.putInt(length, value, LE);
            length += Integer.BYTES;
        }

        /** Offers the accumulated snapshot to the cluster's snapshot publication, retrying under backpressure. */
        void flushTo(ExclusivePublication publication) {
            long result;
            do {
                result = publication.offer(buffer, 0, length);
            } while (result == Publication.BACK_PRESSURED || result == Publication.ADMIN_ACTION);
            if (result < 0L) {
                throw new IllegalStateException("Failed to offer snapshot to cluster, result=" + result);
            }
        }
    }

    /** Drains a snapshot {@link Image} fully into memory, then serves it back as a {@link SnapshotSource}. */
    static final class Reader implements SnapshotSource, FragmentHandler {

        private final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(256);
        private int writeOffset;
        private int readOffset;

        /** Blocks until {@code image} reaches end-of-stream, then returns a ready-to-read snapshot. */
        static Reader drain(Image image) {
            Reader reader = new Reader();
            while (!image.isEndOfStream() && !image.isClosed()) {
                if (image.poll(reader, POLL_FRAGMENT_LIMIT) <= 0) {
                    Thread.onSpinWait();
                }
            }
            return reader;
        }

        @Override
        public void onFragment(DirectBuffer fragment, int offset, int length, Header header) {
            buffer.putBytes(writeOffset, fragment, offset, length);
            writeOffset += length;
        }

        @Override
        public long nextLong() {
            long value = buffer.getLong(readOffset, LE);
            readOffset += Long.BYTES;
            return value;
        }

        @Override
        public int nextInt() {
            int value = buffer.getInt(readOffset, LE);
            readOffset += Integer.BYTES;
            return value;
        }
    }
}
