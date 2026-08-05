package gcm.md.sequencer.stamping;

/**
 * A sequential, append-only primitive stream {@link StampingEngine#writeSnapshot} writes to.
 * {@code cluster-node} (design §5, Milestone 2) implements this over an Aeron cluster snapshot
 * publication; tests use a trivial in-memory implementation. Values must be read back via
 * {@link SnapshotSource} in exactly the order they were written.
 */
public interface SnapshotSink {

    void putLong(long value);

    void putInt(int value);
}
