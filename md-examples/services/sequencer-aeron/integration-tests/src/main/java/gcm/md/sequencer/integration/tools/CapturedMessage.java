package gcm.md.sequencer.integration.tools;

import java.time.Instant;

/**
 * One message captured off either phase's output subject, with the join key
 * ({@code sourceSeqNum}) already decoded from the wire (design §12.4). {@code sourceSeqNum} is
 * never rewritten by either phase's sequencer — only {@code sequenceId}/{@code sequenceTimestamp}
 * are — so it survives unchanged from the synthetic input {@link ParallelRunDiffCli} publishes,
 * through both pipelines, out to both output subjects, which is exactly what makes it a valid
 * join key here without needing correlation ids or timestamps.
 */
record CapturedMessage(long sourceSeqNum, byte[] payload, Instant capturedAt) {
}
