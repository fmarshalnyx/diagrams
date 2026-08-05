package gcm.md.mockupstreamsource.metrics;

import gcm.md.mockupstreamsource.verify.EgressConsumer;
import gcm.md.mockupstreamsource.verify.SequenceVerifier;
import io.micrometer.core.instrument.MeterRegistry;

import java.util.concurrent.atomic.LongAdder;

/**
 * Observability for the continuous generate/verify service. {@code mock_upstream_gap_total}/
 * {@code mock_upstream_duplicate_total} should be permanently zero — the sequencer's own dedup
 * is supposed to absorb every deliberately-injected gap/duplicate this service generates; a
 * nonzero reading here is a real sequencer bug, not routine noise (same contract as
 * {@code bridge_gap_total} elsewhere in this reactor).
 */
public final class MockUpstreamSourceMetrics {

    private final LongAdder publishedTotal = new LongAdder();

    public MockUpstreamSourceMetrics(MeterRegistry registry, SequenceVerifier verifier, EgressConsumer egressConsumer) {
        registry.gauge("mock_upstream_published_total", publishedTotal, LongAdder::sum);
        registry.gauge("mock_upstream_observed_total", egressConsumer, EgressConsumer::observedCount);
        registry.gauge("mock_upstream_gap_total", verifier, SequenceVerifier::gapCount);
        registry.gauge("mock_upstream_duplicate_total", verifier, SequenceVerifier::duplicateCount);
    }

    /** Records one message actually published upstream (including deliberate duplicates, excluding skips). */
    public void onPublished() {
        publishedTotal.increment();
    }
}
