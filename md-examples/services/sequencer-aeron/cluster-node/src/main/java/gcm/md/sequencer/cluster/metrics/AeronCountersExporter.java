package gcm.md.sequencer.cluster.metrics;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.agrona.concurrent.status.CountersReader;

/**
 * Design §17: "Export Aeron's own counters (errors, back-pressure, flow control) via the counters
 * reader → Prometheus in cluster-node." Rather than hand-picking specific typeIds (Aeron's counter
 * catalog is large and not centrally documented), this exports every counter the embedded media
 * driver currently exposes — including the standard ones design §17 calls out by name (client/
 * publisher/subscriber errors, sender/receiver flow-control state, back-pressure indicators) — as
 * one gauge per counter id, tagged by its own label. A dashboard or alert can select the subset it
 * cares about by matching on the {@code label} tag.
 *
 * <p>Snapshot-at-construction: counters that exist when this is built (true for every driver/
 * archive/consensus-module counter — they're all allocated during {@code ClusteredMediaDriver}
 * startup) are exported; each gauge reads its live value at scrape time via
 * {@link CountersReader#getCounterValue(int)}, not a value captured once. Per-session counters
 * that could be allocated later are not picked up — a known simplification, not a claim of full
 * dynamic coverage.
 */
public final class AeronCountersExporter {

    public AeronCountersExporter(MeterRegistry registry, CountersReader countersReader) {
        countersReader.forEach((value, counterId, label) ->
                Gauge.builder("aeron_counter_value", countersReader, cr -> cr.getCounterValue(counterId))
                        .tag("id", Integer.toString(counterId))
                        .tag("label", label)
                        .register(registry));
    }
}
