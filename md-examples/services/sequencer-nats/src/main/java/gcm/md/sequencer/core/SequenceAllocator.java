package gcm.md.sequencer.core;

import gcm.md.sequencer.config.SequencerProperties;
import gcm.md.sequencer.metrics.SequencerMetrics;
import io.nats.client.Connection;
import io.nats.client.JetStreamApiException;
import io.nats.client.KeyValue;
import io.nats.client.api.KeyValueEntry;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * High-water-mark block leasing over a NATS KV bucket (project spec §6). Gaps are permitted, so
 * only the leased-up-to high-water mark is persisted — never a per-message write. The hot-path
 * method {@link #next()} is a plain {@code long} increment with a cheap threshold check; the KV
 * round trip only happens once per {@code block-size} messages, and is proactively triggered
 * ahead of exhaustion so block rollover never stalls the stamping thread on KV RTT.
 *
 * <p>Single writer by design: only the pipeline thread calls {@link #next()}.
 */
public final class SequenceAllocator {

    private final Connection connection;
    private final String kvBucket;
    private final String kvKey;
    private final long blockSize;
    private final double leaseAheadFraction;
    private final LeaderElection leaderElection;
    private final SequencerMetrics metrics;
    private final ExecutorService leaseExecutor = Executors.newSingleThreadExecutor(r -> {
        Thread t = new Thread(r, "sequencer-block-lease");
        t.setDaemon(true);
        return t;
    });

    private KeyValue kv;
    private long current;
    private long blockStart;
    private long leasedHighWater;
    private long leaseAheadThreshold;
    private CompletableFuture<Long> pendingLease;

    /**
     * Creates the allocator; call {@link #initialize()} once before the first {@link #next()}.
     * The {@link KeyValue} handle is obtained lazily inside {@link #initialize()} rather than
     * taken as a constructor argument, since it must not be resolved until after the KV bucket
     * has been created (pipeline startup ordering step 1, before step 3 — see
     * {@link SequencerPipeline}).
     */
    public SequenceAllocator(Connection connection, SequencerProperties properties, LeaderElection leaderElection,
                              SequencerMetrics metrics) {
        this.connection = connection;
        SequencerProperties.Allocator config = properties.getAllocator();
        this.kvBucket = config.getKvBucket();
        this.kvKey = config.getKvKey();
        this.blockSize = config.getBlockSize();
        this.leaseAheadFraction = config.getLeaseAheadFraction();
        this.leaderElection = leaderElection;
        this.metrics = metrics;
    }

    /**
     * Reads the current high-water mark and leases the first block. Startup ordering step 3
     * (§10): must run after leadership is acquired and before ingress subscribes.
     */
    public void initialize() {
        try {
            this.kv = connection.keyValue(kvBucket);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to open allocator KV bucket: " + kvBucket, e);
        }
        long highWater = readHighWaterMark();
        this.current = highWater;
        this.blockStart = highWater;
        this.leasedHighWater = highWater;
        adoptNewBlock(casLeaseBlock(highWater, highWater + blockSize));
    }

    /**
     * Hot path: returns the next sequenceId. Transparently leases new blocks as needed, blocking
     * only in the rare case that a proactively-leased block hasn't arrived by the time the
     * current one is exhausted.
     */
    public long next() {
        long id = ++current;
        if (id > leasedHighWater) {
            blockForNextBlock();
        } else if (id == leaseAheadThreshold && pendingLease == null) {
            pendingLease = CompletableFuture.supplyAsync(
                    () -> casLeaseBlock(leasedHighWater, leasedHighWater + blockSize), leaseExecutor);
        }
        return id;
    }

    private void blockForNextBlock() {
        long newHighWater;
        if (pendingLease != null) {
            newHighWater = pendingLease.join();
            pendingLease = null;
        } else {
            newHighWater = casLeaseBlock(leasedHighWater, leasedHighWater + blockSize);
        }
        adoptNewBlock(newHighWater);
    }

    private void adoptNewBlock(long newHighWater) {
        this.blockStart = this.leasedHighWater;
        this.leasedHighWater = newHighWater;
        long size = newHighWater - blockStart;
        this.leaseAheadThreshold = blockStart + Math.round(size * leaseAheadFraction);
        metrics.incrementBlocksLeased();
    }

    private long readHighWaterMark() {
        try {
            KeyValueEntry entry = kv.get(kvKey);
            return entry == null ? 0L : Long.parseLong(entry.getValueAsString());
        } catch (IOException | JetStreamApiException e) {
            throw new IllegalStateException("Failed to read allocator high-water mark from KV bucket", e);
        }
    }

    /**
     * Compare-and-swap lease of the next block: fences on leadership, then retries until the KV
     * write succeeds (self-healing off whatever value is actually stored if a concurrent writer
     * moved it, which under correct fencing should only ever be this process itself).
     */
    private long casLeaseBlock(long expectedCurrentHighWater, long target) {
        if (!leaderElection.isLeader()) {
            throw new IllegalStateException("Refusing to lease a sequence block: leadership not held");
        }
        long expected = expectedCurrentHighWater;
        while (true) {
            try {
                KeyValueEntry entry = kv.get(kvKey);
                long stored = entry == null ? 0L : Long.parseLong(entry.getValueAsString());
                if (stored != expected) {
                    expected = stored;
                }
                long newTarget = expected + blockSize;
                byte[] value = Long.toString(newTarget).getBytes(StandardCharsets.UTF_8);
                if (entry == null) {
                    kv.create(kvKey, value);
                } else {
                    kv.update(kvKey, value, entry.getRevision());
                }
                return newTarget;
            } catch (JetStreamApiException e) {
                // Lost a CAS race (split-brain attempt, or a stale fencing window) — reread and retry.
                metrics.incrementLeaseFailures();
            } catch (IOException e) {
                metrics.incrementLeaseFailures();
                throw new IllegalStateException("Failed to lease sequence block from KV bucket", e);
            }
        }
    }

    /** Returns the last sequenceId issued by {@link #next()} (0 if none issued yet). */
    public long lastAssigned() {
        return current;
    }

    /** Shuts down the background lease-ahead executor. */
    public void stop() {
        leaseExecutor.shutdownNow();
    }
}
