package com.trading.sequencer;

import io.nats.client.*;
import io.nats.client.api.*;
import io.nats.client.impl.Headers;
import io.nats.client.impl.NatsMessage;

import java.time.Duration;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Assigns a globally unique, order-of-receipt sequenceId to every market
 * data message, with a warm standby that can take over without losing,
 * duplicating, or gapping a sequenceId.
 *
 * IMPORTANT - only ONE JetStream leg remains on the critical path:
 *
 *   Line handlers --(core NATS, "md.raw.>")--> Sequencer --(JetStream, async)--> SEQ_OUT
 *                                                                                    |
 *                                                                    republish (core NATS)
 *                                                                                    v
 *                                                                     downstream / warm standby
 *
 * Why raw ingest is core NATS, not JetStream:
 * The only thing JetStream on the ingest leg bought us was "don't lose a raw
 * message if the sequencer dies." We get that same guarantee more cheaply
 * via fan-out instead of replication: BOTH the active and standby instance
 * subscribe to "md.raw.>" directly and continuously, so the standby already
 * has its own independent copy of every recent raw message in memory. That
 * removes an entire consensus round-trip (publish-with-quorum-ack, pull
 * fetch, explicit ack) from every message's path. The tradeoff, stated
 * plainly: this does NOT protect against both sequencer instances dying in
 * the same brief window, since the raw message was never durably replicated
 * anywhere. If correlated dual-failure is a risk you must cover, put
 * MD_RAW back on JetStream specifically and accept the extra latency there.
 *
 * Why the output publish is async, not blocking:
 * SEQ_OUT must stay JetStream - it's the durable source of truth failover
 * depends on. But blocking on each publish's quorum ack before processing
 * the next message caps throughput at 1/RTT. js.publishAsync() pipelines
 * writes; NATS preserves send order on the connection, so SEQ_OUT's append
 * order still matches receipt order even though we don't wait for each ack.
 *
 * Core invariant, unchanged: SEQ_OUT is the durable source of truth for
 * "what sequenceId did we last assign." The in-process counter and the
 * dedupe window are both just caches - kept warm continuously via a core
 * NATS republish subscription, and verified (not blindly trusted) on every
 * promotion to leader.
 */
public class Sequencer implements AutoCloseable {

    private static final String MD_RAW_SUBJECT = "md.raw.>";           // core NATS, no persistence
    private static final String SEQ_OUT_STREAM = "SEQ_OUT";
    private static final String SEQ_OUT_SUBJECT = "md.sequenced";       // JetStream, durable, R=3
    private static final String SEQ_OUT_LIVE_SUBJECT = "md.sequenced.live"; // core NATS republish target
    private static final String CORRELATION_HEADER = "Correlation-Id";
    private static final String SEQUENCE_HEADER = "Sequence-Id";

    // Cap on the dedupe cache (correlation IDs already known to be sequenced)
    // and the raw-message replay buffer. Must comfortably exceed the number
    // of raw messages that can arrive during a realistic failover-detection
    // window (lease TTL + one renewal interval, see LeaderElector), with
    // headroom for burst rate. Tune against measured message rates.
    private static final int WINDOW_CAPACITY = 5_000;

    private final Connection nc;
    private final JetStream js;
    private final JetStreamManagement jsm;
    private final LeaderElector elector;

    private final AtomicLong sequenceCounter = new AtomicLong(0);

    // Single lock guarding both the raw-message replay buffer and the
    // "am I currently allowed to sequence" decision. Holding it across a
    // full onBecomeLeader() replay means any raw message that arrives
    // mid-promotion queues behind the replay and is necessarily processed
    // after it - preserving receipt order across the handover.
    private final Object bufferLock = new Object();
    private final LinkedHashMap<String, byte[]> rawBuffer = boundedInsertionOrderMap();
    private volatile boolean leading = false;

    // Warm cache of correlation IDs already reflected in SEQ_OUT, kept
    // current by the republish tailer (and, while leading, by our own
    // publishes looping back through that same subscription).
    private final Object dedupeLock = new Object();
    private final LinkedHashSet<String> dedupeWindow = new LinkedHashSet<>();

    public Sequencer(Connection nc, String instanceId) throws Exception {
        this.nc = nc;
        // publishAsyncMaxPending bounds how many un-acked async publishes can
        // be in flight before publishAsync() applies backpressure - without
        // it a slow/partitioned NATS cluster could let this grow unbounded.
        // Verify this option name against your pinned jnats version.
        this.js = nc.jetStream(JetStreamOptions.builder().publishAsyncMaxPending(10_000).build());
        this.jsm = nc.jetStreamManagement();

        this.elector = new LeaderElector(
                nc,
                "sequencer-leader",
                instanceId,
                Duration.ofSeconds(2),        // lease TTL
                Duration.ofMillis(500),       // renew interval
                this::onBecomeLeader,
                this::onLoseLeadership
        );
    }

    private static LinkedHashMap<String, byte[]> boundedInsertionOrderMap() {
        return new LinkedHashMap<String, byte[]>(16, 0.75f, false) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, byte[]> eldest) {
                return size() > WINDOW_CAPACITY;
            }
        };
    }

    public void start() throws Exception {
        ensureSeqOutStream();
        startWarmTailer();
        startRawSubscriber();
        elector.start();
    }

    private void ensureSeqOutStream() throws Exception {
        try {
            jsm.addStream(StreamConfiguration.builder()
                    .name(SEQ_OUT_STREAM)
                    .subjects(SEQ_OUT_SUBJECT)
                    .storageType(StorageType.File)
                    .replicas(3)
                    // Every message persisted here is also re-emitted, best
                    // effort, as a plain core NATS message - no consumer/ack
                    // state, essentially wire speed. Used purely to keep
                    // caches warm; SEQ_OUT itself is still the source of truth.
                    .republish(Republish.builder()
                            .source(SEQ_OUT_SUBJECT)
                            .destination(SEQ_OUT_LIVE_SUBJECT)
                            .headersOnly(false)
                            .build())
                    .build());
        } catch (JetStreamApiException e) {
            // already exists
        }
    }

    /** Core NATS, not JetStream: keeps dedupeWindow/sequenceCounter warm on every instance. */
    private void startWarmTailer() throws Exception {
        Dispatcher d = nc.createDispatcher(this::onWarmTailMessage);
        d.subscribe(SEQ_OUT_LIVE_SUBJECT);
    }

    private void onWarmTailMessage(Message m) {
        Headers h = m.getHeaders();
        if (h == null) return;
        String corr = h.getFirst(CORRELATION_HEADER);
        if (corr != null) addDedupe(corr);
        String seqIdStr = h.getFirst(SEQUENCE_HEADER);
        if (seqIdStr != null) {
            try {
                advanceCounterIfHigher(Long.parseLong(seqIdStr));
            } catch (NumberFormatException ignored) {
            }
        }
    }

    /**
     * Core NATS, not JetStream: this is the entire raw ingest path. Runs on
     * every instance regardless of leadership. When following, messages are
     * only buffered (so we're warm if promoted). When leading, messages are
     * buffered AND sequenced immediately - buffering while leading is cheap
     * and keeps the code path uniform.
     */
    private void startRawSubscriber() throws Exception {
        Dispatcher d = nc.createDispatcher(this::onRawMessage);
        d.subscribe(MD_RAW_SUBJECT);
    }

    private void onRawMessage(Message m) {
        Headers h = m.getHeaders();
        String correlationId = h != null ? h.getFirst(CORRELATION_HEADER) : null;
        if (correlationId == null) {
            // Malformed input from a line handler - not a sequencer concern.
            // Route to a dead-letter subject / metric in a real deployment.
            return;
        }

        synchronized (bufferLock) {
            rawBuffer.put(correlationId, m.getData());
            if (leading) {
                sequenceAndPublish(correlationId, m.getData());
            }
        }
    }

    // --- Leadership transitions -------------------------------------------

    private void onBecomeLeader() {
        try {
            recoverState();
        } catch (Exception e) {
            // Must NOT start sequencing without a trustworthy recovered
            // counter/dedupe set - that risks duplicates or gaps. Fail loud.
            throw new IllegalStateException("Recovery failed, refusing to become active", e);
        }

        synchronized (bufferLock) {
            // Replay everything received while we were a follower, in the
            // order it was received, before accepting new work. Holding
            // bufferLock for the duration means any raw message arriving
            // concurrently on the dispatcher thread queues behind this and
            // is correctly treated as "newer than everything buffered."
            for (Map.Entry<String, byte[]> entry : rawBuffer.entrySet()) {
                sequenceAndPublish(entry.getKey(), entry.getValue());
            }
            leading = true;
        }
    }

    private void onLoseLeadership() {
        synchronized (bufferLock) {
            leading = false;
        }
    }

    /**
     * Sequences and publishes exactly once per correlation ID. Safe to call
     * both during buffer replay on promotion and during live processing -
     * dedupeContains() catches anything already reflected in SEQ_OUT
     * (whether from a prior leader's tenure or an earlier call in this
     * same replay).
     */
    private void sequenceAndPublish(String correlationId, byte[] payload) {
        if (dedupeContains(correlationId)) {
            return; // already durably sequenced - this is a redelivery/replay duplicate
        }

        long sequenceId = sequenceCounter.incrementAndGet();
        addDedupe(correlationId); // reserve immediately so a concurrent duplicate can't double-assign

        Headers outHeaders = new Headers()
                .add(SEQUENCE_HEADER, String.valueOf(sequenceId))
                .add(CORRELATION_HEADER, correlationId);

        NatsMessage outMsg = NatsMessage.builder()
                .subject(SEQ_OUT_SUBJECT)
                .headers(outHeaders)
                .data(payload)
                .build();

        CompletableFuture<PublishAck> future = js.publishAsync(outMsg);
        future.whenComplete((ack, err) -> {
            if (err != null) {
                // The sequenceId was assigned locally but never durably
                // persisted - this leaves a permanent gap in SEQ_OUT (not a
                // duplicate, which is the safer failure direction, but still
                // needs visibility). Replace with real logging/metrics/
                // alerting; a sustained failure rate here should trigger
                // this instance voluntarily stepping down as leader rather
                // than continuing to assign IDs it can't persist.
                System.err.println("SEQ_OUT publish failed for correlationId=" + correlationId + ": " + err);
            }
        });
    }

    /**
     * Verifies (and if necessary rebuilds) in-memory state against the
     * durable output log before this instance is allowed to start
     * sequencing. In the common case the warm tailer already kept
     * sequenceCounter/dedupeWindow current, so this is one cheap read of
     * SEQ_OUT's real tail to confirm we're caught up - not a full scan.
     */
    private void recoverState() throws Exception {
        StreamInfo info = jsm.getStreamInfo(SEQ_OUT_STREAM);
        long lastSeq = info.getStreamState().getLastSequence();
        if (lastSeq == 0) return; // stream empty - counter correctly starts at 0

        MessageInfo last = jsm.getMessage(SEQ_OUT_STREAM, lastSeq);
        Headers h = last.getHeaders();
        if (h != null) {
            String seqIdStr = h.getFirst(SEQUENCE_HEADER);
            if (seqIdStr != null) advanceCounterIfHigher(Long.parseLong(seqIdStr));
            String corr = h.getFirst(CORRELATION_HEADER);
            if (corr != null) addDedupe(corr);
        }

        if (dedupeSize() == 0) {
            // Cache was genuinely cold (fresh process, tailer hasn't caught
            // up yet) - reconstruct it directly from the log before trusting it.
            backfillFromLog(lastSeq);
        }
    }

    private void backfillFromLog(long lastSeq) throws Exception {
        long start = Math.max(1, lastSeq - WINDOW_CAPACITY + 1);
        Set<String> recent = new LinkedHashSet<>();
        long lastAssigned = sequenceCounter.get();

        for (long seq = start; seq <= lastSeq; seq++) {
            MessageInfo mi;
            try {
                mi = jsm.getMessage(SEQ_OUT_STREAM, seq);
            } catch (JetStreamApiException e) {
                continue; // trimmed by retention policy - outside our window of interest
            }
            Headers h = mi.getHeaders();
            if (h != null) {
                String corr = h.getFirst(CORRELATION_HEADER);
                if (corr != null) recent.add(corr);
                String seqIdStr = h.getFirst(SEQUENCE_HEADER);
                if (seqIdStr != null) lastAssigned = Math.max(lastAssigned, Long.parseLong(seqIdStr));
            }
        }

        advanceCounterIfHigher(lastAssigned);
        resetDedupe(recent);
    }

    // --- Dedupe window helpers (thread-safe, bounded) -----------------------

    private void addDedupe(String correlationId) {
        synchronized (dedupeLock) {
            dedupeWindow.add(correlationId);
            while (dedupeWindow.size() > WINDOW_CAPACITY) {
                Iterator<String> it = dedupeWindow.iterator();
                it.next();
                it.remove(); // LinkedHashSet insertion order - evicts the oldest
            }
        }
    }

    private boolean dedupeContains(String correlationId) {
        synchronized (dedupeLock) {
            return dedupeWindow.contains(correlationId);
        }
    }

    private int dedupeSize() {
        synchronized (dedupeLock) {
            return dedupeWindow.size();
        }
    }

    private void resetDedupe(Set<String> values) {
        synchronized (dedupeLock) {
            dedupeWindow.clear();
            dedupeWindow.addAll(values);
        }
    }

    private void advanceCounterIfHigher(long candidate) {
        long prev;
        do {
            prev = sequenceCounter.get();
            if (candidate <= prev) return;
        } while (!sequenceCounter.compareAndSet(prev, candidate));
    }

    @Override
    public void close() {
        synchronized (bufferLock) {
            leading = false;
        }
        elector.close();
    }

    // --- Wiring --------------------------------------------------------------

    public static void main(String[] args) throws Exception {
        String natsUrl = System.getenv().getOrDefault("NATS_URL", "nats://localhost:4222");
        String instanceId = System.getenv().getOrDefault("POD_NAME", "sequencer-" + System.currentTimeMillis());

        Options options = new Options.Builder()
                .server(natsUrl)
                .connectionListener((conn, type) -> System.out.println("[nats] " + type))
                .build();

        try (Connection nc = Nats.connect(options)) {
            Sequencer sequencer = new Sequencer(nc, instanceId);
            sequencer.start();

            Runtime.getRuntime().addShutdownHook(new Thread(sequencer::close));

            // Keep the process alive; leadership state is driven entirely by
            // the LeaderElector's background thread.
            Thread.currentThread().join();
        }
    }
}
