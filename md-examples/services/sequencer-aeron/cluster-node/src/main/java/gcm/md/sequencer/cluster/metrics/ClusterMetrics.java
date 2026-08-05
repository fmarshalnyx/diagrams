package gcm.md.sequencer.cluster.metrics;

import gcm.md.sequencer.cluster.ClusterServiceListener;
import gcm.md.sequencer.cluster.EgressListener;
import gcm.md.sequencer.cluster.SourcePrincipal;
import gcm.md.sequencer.stamping.EngineListener;
import io.aeron.cluster.service.Cluster;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;

/**
 * All cluster-node observability in one place (design §17), the phase-2 counterpart of phase-1's
 * {@code SequencerMetrics}. Implements every "reports through callbacks, never touched directly
 * by the logic it's observing" listener interface this module defines
 * ({@link EngineListener}, {@link EgressListener}, {@link ClusterServiceListener}) so
 * {@link gcm.md.sequencer.cluster.ClusterNodeLauncher} can wire one bean everywhere metrics are
 * needed. Every method here is called from the clustered service's single thread — plain field
 * writes only, no blocking, no allocation beyond the one-time per-source counter registration.
 *
 * <p>{@code sequencer_dr_replication_lag_sequences} is registered but not yet fed a real value:
 * DR Archive replication doesn't exist until Milestone 12's prod-dr work. It stays at 0 until
 * that component calls {@link #setDrReplicationLagSequences(long)} — present now so dashboards
 * and alerts can be built against the name ahead of time, not a claim that DR lag is measured
 * today.
 */
public final class ClusterMetrics implements EngineListener, EgressListener, ClusterServiceListener {

    private final MeterRegistry registry;
    private final Map<Long, String> sourceNamesById;

    private final Map<String, Counter> sourceDuplicateCounters = new ConcurrentHashMap<>();
    private final Map<String, Counter> sourceSeqGapCounters = new ConcurrentHashMap<>();

    private final LongAdder egressSuppressedTotal = new LongAdder();
    private final LongAdder backpressureStallNanosTotal = new LongAdder();
    private final LongAdder schemaMismatchTotal = new LongAdder();
    private final LongAdder eventTrackingEvictedTotal = new LongAdder();

    private final DistributionSummary snapshotDurationSeconds;

    private volatile int clusterRoleValue;
    private volatile long commitPosition;
    private volatile long drReplicationLagSequences;

    public ClusterMetrics(MeterRegistry registry, List<SourcePrincipal> sources) {
        this.registry = registry;
        this.sourceNamesById = sources.stream()
                .collect(Collectors.toUnmodifiableMap(SourcePrincipal::sourceId, SourcePrincipal::name));

        registry.gauge("sequencer_cluster_role", this, m -> m.clusterRoleValue);
        registry.gauge("sequencer_commit_position", this, m -> m.commitPosition);
        registry.gauge("sequencer_egress_suppressed_total", egressSuppressedTotal, LongAdder::sum);
        registry.gauge("sequencer_backpressure_stall_seconds_total", backpressureStallNanosTotal,
                adder -> adder.sum() / 1_000_000_000.0);
        registry.gauge("sequencer_schema_mismatch_total", schemaMismatchTotal, LongAdder::sum);
        registry.gauge("sequencer_event_tracking_evicted_total", eventTrackingEvictedTotal, LongAdder::sum);
        registry.gauge("sequencer_dr_replication_lag_sequences", this, m -> m.drReplicationLagSequences);
        this.snapshotDurationSeconds = DistributionSummary.builder("sequencer_snapshot_duration_seconds")
                .description("Wall-clock duration of onTakeSnapshot")
                .register(registry);
    }

    // -- ClusterServiceListener --

    @Override
    public void onRoleChange(Cluster.Role newRole) {
        clusterRoleValue = newRole.code(); // Aeron's own FOLLOWER=0/CANDIDATE=1/LEADER=2 encoding
    }

    @Override
    public void onSnapshotTaken(long durationNanos) {
        snapshotDurationSeconds.record(durationNanos / 1_000_000_000.0);
    }

    @Override
    public void onCommitPositionSample(long position) {
        commitPosition = position;
    }

    // -- EgressListener --

    @Override
    public void onSuppressed(long sequenceId) {
        egressSuppressedTotal.increment();
    }

    @Override
    public void onBackpressureStall(long nanos) {
        backpressureStallNanosTotal.add(nanos);
    }

    // -- EngineListener --

    @Override
    public void onSchemaMismatch() {
        schemaMismatchTotal.increment();
    }

    @Override
    public void onEventTrackingEvicted() {
        eventTrackingEvictedTotal.increment();
    }

    @Override
    public void onSourceDuplicate(long sourceId) {
        counterFor(sourceDuplicateCounters, "sequencer_source_duplicate_total", sourceId).increment();
    }

    @Override
    public void onSourceSeqGap(long sourceId, long gapSize) {
        counterFor(sourceSeqGapCounters, "sequencer_source_seq_gap_total", sourceId).increment(gapSize);
    }

    /** Not yet wired to a real signal — see class Javadoc. */
    public void setDrReplicationLagSequences(long lagSequences) {
        this.drReplicationLagSequences = lagSequences;
    }

    private Counter counterFor(Map<String, Counter> counters, String metricName, long sourceId) {
        String source = sourceNamesById.getOrDefault(sourceId, "unknown-" + sourceId);
        return counters.computeIfAbsent(source, s -> Counter.builder(metricName).tag("source", s).register(registry));
    }
}
