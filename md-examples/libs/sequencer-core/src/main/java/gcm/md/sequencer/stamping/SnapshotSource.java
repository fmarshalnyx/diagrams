package gcm.md.sequencer.stamping;

/**
 * A sequential primitive stream {@link StampingEngine#loadSnapshot} reads from — the read-side
 * counterpart of {@link SnapshotSink}. Values must be supplied in exactly the order
 * {@link StampingEngine#writeSnapshot} wrote them.
 */
public interface SnapshotSource {

    long nextLong();

    int nextInt();
}
