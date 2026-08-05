# Observability (design §17, implementation-steps.md Milestone 10)

`gcm-md-sequencer-aeron-dashboard.json` — a Grafana dashboard covering the whole phase-2
pipeline (`mock-upstream-source` -> `line-handler-template` -> `cluster-node` -> `nats-bridge` ->
`md.sequenced` -> `mock-upstream-source`'s own verifier), built directly against the metrics each
service already exposes (no new instrumentation needed to use it).

## Prerequisite: cluster-node must actually be scraped

`cluster-node` has no Spring dependency (design §3.3), so its metrics come from a minimal JDK
`HttpServer` (`MetricsHttpServer`) on `/metrics`, not `/actuator/prometheus` — a separate
endpoint from the other three services. This wasn't wired into the Helm chart until this
dashboard was written (no container port, no Service port, no ServiceMonitor existed for it
before — see `templates/clusternode/{statefulset,headless-service}.yaml`,
`templates/servicemonitor.yaml`, and `templates/networkpolicy.yaml`, all updated in the same
change as this dashboard). If panels in the "Cluster leadership & health" or "Snapshot &
consensus internals" rows show "No data," confirm `serviceMonitor.enabled: true` and that
`clusterNode.ports.metrics` (9100 by default) is actually reachable from Prometheus.

## Importing

1. Grafana → Dashboards → New → Import → upload `gcm-md-sequencer-aeron-dashboard.json`.
2. When prompted, select the Prometheus datasource scraping this chart's `ServiceMonitor`s.
3. The `environment` template variable is populated from the `environment` Micrometer tag
   (`management.metrics.tags.environment` in each service's `application.yml` /
   `application-<profile>.yml`) — defaults to `unknown` unless a profile sets it.

## A naming gotcha if you extend this dashboard

Every metric registered via `MeterRegistry.gauge(name, ...)` in this codebase has its Java-side
name end in `_total` (e.g. `bridge_gap_total`, matching Prometheus counter-naming convention at
the call site) — but Micrometer's Prometheus exporter strips that suffix for gauge-typed meters
on scrape (`_total` is reserved for actual counters), so the metric Prometheus actually stores is
`bridge_gap`, not `bridge_gap_total`. Metrics registered via `Counter.builder(...)` (only
`sequencer_source_duplicate_total`/`sequencer_source_seq_gap_total`, both per-source in
`ClusterMetrics`/phase-1's `SequencerMetrics`) are real counters and *do* keep `_total`. Confirmed
directly against a live scrape (`curl .../actuator/prometheus` / `.../metrics`, `grep '^# TYPE'`)
while building this dashboard — check the actual `# TYPE` line for a metric before assuming its
Java-side registration name is what you'll find in Prometheus.

## What's deliberately not here

No alerting rules — this is a dashboard only. The "should be permanently zero" metrics called out
throughout (`bridge_gap`, `mock_upstream_gap`, `mock_upstream_duplicate`,
`sequencer_schema_mismatch`, `sequencer_source_duplicate_total`,
`sequencer_source_seq_gap_total`) are the natural candidates for alert rules once this project has
a real Alertmanager target — not added speculatively here.
