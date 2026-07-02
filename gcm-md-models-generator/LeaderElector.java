package com.trading.sequencer;

import io.nats.client.*;
import io.nats.client.api.KeyValueConfiguration;
import io.nats.client.api.StorageType;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Lease-based leader election on top of a JetStream KV bucket.
 *
 * Design notes:
 * - The lease value encodes "<instanceId>|<expiresAtEpochMillis>" rather than
 *   relying on NATS server-side per-key TTL (that feature requires NATS
 *   Server 2.11+; encoding expiry ourselves works on any JetStream version
 *   and keeps the failover logic fully in our control).
 * - kv.create() / kv.update(key, value, expectedRevision) give us the
 *   compare-and-swap primitive that makes acquisition/renewal race-safe:
 *   only one process can win a given revision.
 * - This is a SEPARATE, softer signal from the durable-consumer exclusivity
 *   fencing described in the design doc. Use both: this decides *when* the
 *   standby should attempt takeover; the durable consumer bind is the hard
 *   backstop against two processes actually consuming at once.
 */
public class LeaderElector implements AutoCloseable {

    private static final String LEADER_KEY = "leader";

    private final KeyValue kv;
    private final String instanceId;
    private final long leaseTtlMillis;
    private final Duration renewInterval;
    private final Runnable onBecomeLeader;
    private final Runnable onLoseLeadership;

    private final AtomicBoolean isLeader = new AtomicBoolean(false);
    private final AtomicLong knownRevision = new AtomicLong(-1);
    private ScheduledExecutorService scheduler;

    public LeaderElector(Connection nc,
                          String bucket,
                          String instanceId,
                          Duration leaseTtl,
                          Duration renewInterval,
                          Runnable onBecomeLeader,
                          Runnable onLoseLeadership) throws Exception {
        this.instanceId = instanceId;
        this.leaseTtlMillis = leaseTtl.toMillis();
        this.renewInterval = renewInterval;
        this.onBecomeLeader = onBecomeLeader;
        this.onLoseLeadership = onLoseLeadership;

        KeyValueManagement kvm = nc.keyValueManagement();
        try {
            kvm.create(KeyValueConfiguration.builder()
                    .name(bucket)
                    .storageType(StorageType.File)
                    .replicas(3)
                    .build());
        } catch (JetStreamApiException e) {
            // Bucket already exists from a previous run / other instance — fine.
        }
        this.kv = nc.keyValue(bucket);
    }

    public boolean isLeader() {
        return isLeader.get();
    }

    public void start() {
        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "leader-elector-" + instanceId);
            t.setDaemon(true);
            return t;
        });
        scheduler.scheduleAtFixedRate(this::tick, 0, renewInterval.toMillis(), TimeUnit.MILLISECONDS);
    }

    private void tick() {
        try {
            if (isLeader.get()) {
                renew();
            } else {
                tryAcquire();
            }
        } catch (Exception e) {
            // Any error talking to NATS during a leadership tick is treated as a
            // potential loss of leadership. Fail safe (give up leadership),
            // never fail open (assume we're still leader when unsure).
            if (isLeader.compareAndSet(true, false)) {
                onLoseLeadership.run();
            }
        }
    }

    private void tryAcquire() throws Exception {
        KeyValueEntry existing = safeGet();

        if (existing == null) {
            // Nobody holds the lease yet.
            try {
                long rev = kv.create(LEADER_KEY, encode(now() + leaseTtlMillis));
                claim(rev);
            } catch (JetStreamApiException e) {
                // Someone beat us to it in the race between get() and create().
            }
            return;
        }

        long[] parsed = decode(existing);
        long expiresAt = parsed[1];
        boolean weAreRecordedHolder = holderIs(existing, instanceId);

        if (now() < expiresAt && !weAreRecordedHolder) {
            return; // Someone else's lease is still valid.
        }

        // Lease expired, or it's stale state naming us (e.g. we restarted after
        // a crash) — attempt to take it via CAS on the last known revision.
        try {
            long rev = kv.update(LEADER_KEY, encode(now() + leaseTtlMillis), existing.getRevision());
            claim(rev);
        } catch (JetStreamApiException e) {
            // Another follower's CAS won the race — stay a follower.
        }
    }

    private void renew() throws Exception {
        try {
            long rev = kv.update(LEADER_KEY, encode(now() + leaseTtlMillis), knownRevision.get());
            knownRevision.set(rev);
        } catch (JetStreamApiException e) {
            // Our lease lapsed (e.g. GC pause, network partition) and someone
            // else already took over.
            if (isLeader.compareAndSet(true, false)) {
                onLoseLeadership.run();
            }
        }
    }

    private void claim(long revision) {
        knownRevision.set(revision);
        if (isLeader.compareAndSet(false, true)) {
            onBecomeLeader.run();
        }
    }

    private KeyValueEntry safeGet() throws Exception {
        try {
            return kv.get(LEADER_KEY);
        } catch (Exception e) {
            return null;
        }
    }

    private boolean holderIs(KeyValueEntry entry, String id) {
        String v = new String(entry.getValue(), StandardCharsets.UTF_8);
        return v.startsWith(id + "|");
    }

    private byte[] encode(long expiresAtMillis) {
        return (instanceId + "|" + expiresAtMillis).getBytes(StandardCharsets.UTF_8);
    }

    private long[] decode(KeyValueEntry entry) {
        String v = new String(entry.getValue(), StandardCharsets.UTF_8);
        String[] parts = v.split("\\|", 2);
        return new long[]{0L, Long.parseLong(parts[1])};
    }

    private long now() {
        return System.currentTimeMillis();
    }

    @Override
    public void close() {
        if (scheduler != null) {
            scheduler.shutdownNow();
        }
        if (isLeader.get()) {
            try {
                kv.delete(LEADER_KEY); // best-effort: release now instead of waiting out the lease
            } catch (Exception ignored) {
            }
        }
    }
}
