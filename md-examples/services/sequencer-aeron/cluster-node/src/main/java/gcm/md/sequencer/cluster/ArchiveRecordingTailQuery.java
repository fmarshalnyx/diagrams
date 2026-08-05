package gcm.md.sequencer.cluster;

import io.aeron.Aeron;
import io.aeron.Image;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import org.agrona.CloseHelper;
import org.agrona.collections.MutableLong;

import java.nio.ByteOrder;

/**
 * {@link RecordingTailQuery} backed by a real Aeron Archive (design §6.3, §6.4). Finds the
 * egress recording by a fixed, well-known session id (design: {@link AeronEgressPublisher} always
 * requests this exact session id for the egress publication, specifically so this query can find
 * the same logical recording across restarts and leader changes without any extra
 * cross-instance state to track), then replays it end to end, keeping the sequenceId of the last
 * fragment seen.
 *
 * <p><b>Confidence note:</b> this is the highest-risk class in the module. It was written
 * against the Aeron Archive client API (verified via the {@code aeron-all} sources jar) but has
 * not been exercised against a live Archive — no cluster in this environment could be started to
 * validate it end to end. The design doc's own §12.2 leader-kill integration test (kill the
 * leader mid-stream, assert no sequenceId appears twice and none is skipped on recorded egress)
 * is exactly the test that must pass before this is trusted in a real failover; treat that test,
 * once written in {@code services/sequencer-aeron/integration-tests}, as a gate on this class,
 * not a formality.
 */
final class ArchiveRecordingTailQuery implements RecordingTailQuery {

    private static final ByteOrder LE = ByteOrder.LITTLE_ENDIAN;
    private static final int REPLAY_STREAM_ID_OFFSET = 1_000; // avoid colliding with the live egress stream id
    private static final int POLL_FRAGMENT_LIMIT = 10;

    private final AeronArchive archive;
    private final Aeron aeron;
    private final AeronEgressConfig config;

    ArchiveRecordingTailQuery(AeronArchive archive, Aeron aeron, AeronEgressConfig config) {
        this.archive = archive;
        this.aeron = aeron;
        this.config = config;
    }

    @Override
    public long lastPublishedSequenceId() {
        long recordingId = archive.findLastMatchingRecording(0L, config.egressChannel(), config.egressStreamId(),
                AeronEgressPublisher.EGRESS_SESSION_ID);
        if (recordingId == Aeron.NULL_VALUE) {
            return SuppressionGate.NO_SUPPRESSION;
        }

        MutableLong startPosition = new MutableLong();
        MutableLong stopPosition = new MutableLong();
        archive.listRecording(recordingId, (controlSessionId, correlationId, foundRecordingId, startTimestamp,
                stopTimestamp, foundStartPosition, foundStopPosition, initialTermId, segmentFileLength,
                termBufferLength, mtuLength, sessionId, streamId, strippedChannel, originalChannel, sourceIdentity) -> {
            startPosition.set(foundStartPosition);
            stopPosition.set(foundStopPosition);
        });

        if (stopPosition.get() <= startPosition.get()) {
            return SuppressionGate.NO_SUPPRESSION; // recording exists but is empty
        }

        int replayStreamId = config.egressStreamId() + REPLAY_STREAM_ID_OFFSET;
        String replayChannel = "aeron:ipc";
        long replaySessionId = archive.startReplay(recordingId, startPosition.get(),
                stopPosition.get() - startPosition.get(), replayChannel, replayStreamId);

        Subscription replaySubscription = aeron.addSubscription(replayChannel, replayStreamId);
        try {
            MutableLong lastSequenceId = new MutableLong(SuppressionGate.NO_SUPPRESSION);
            Image image = awaitReplayImage(replaySubscription, (int) replaySessionId);
            while (!image.isEndOfStream() && !image.isClosed() && image.position() < stopPosition.get()) {
                if (image.poll((buffer, offset, length, header) ->
                        lastSequenceId.set(buffer.getLong(offset + config.sequenceIdOffset(), LE)),
                        POLL_FRAGMENT_LIMIT) <= 0) {
                    Thread.onSpinWait();
                }
            }
            return lastSequenceId.get();
        } finally {
            CloseHelper.quietClose(replaySubscription);
        }
    }

    private static Image awaitReplayImage(Subscription subscription, int sessionId) {
        Image image;
        while ((image = subscription.imageBySessionId(sessionId)) == null) {
            Thread.onSpinWait();
        }
        return image;
    }
}
